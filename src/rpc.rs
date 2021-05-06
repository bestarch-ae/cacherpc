use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use actix::prelude::Addr;
use actix_web::{web, Error, HttpResponse};

use awc::Client;
use bytes::Bytes;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use tokio::sync::{Notify, Semaphore};
use tracing::info;

use crate::accounts::{AccountCommand, AccountUpdateManager};
use crate::types::{AccountContext, AccountData, AccountInfo, Pubkey, SolanaContext};

impl AccountInfo {
    fn encode(&self, encoding: Encoding, slice: Option<Slice>) -> EncodedAccountInfo {
        EncodedAccountInfo {
            lamports: self.lamports,
            owner: self.owner,
            executable: self.executable,
            rent_epoch: self.rent_epoch,
            data: encoding.with_account_data(&self.data).slice(slice),
        }
    }
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone)]
enum Encoding {
    #[serde(rename = "base58")]
    Base58,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

impl Encoding {
    fn with_account_data(self, data: &'_ AccountData) -> EncodedAccountData<'_> {
        EncodedAccountData {
            encoding: self,
            data,
            slice: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct Slice {
    offset: usize,
    length: usize,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
struct EncodedAccountInfo<'a> {
    lamports: u64,
    data: EncodedAccountData<'a>,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
}

impl<'a> EncodedAccountInfo<'a> {
    fn with_context(self, ctx: &'a SolanaContext) -> EncodedAccountContext<'a> {
        EncodedAccountContext {
            value: self,
            context: ctx,
        }
    }
}

#[derive(Serialize, Debug)]
struct EncodedAccountContext<'a> {
    context: &'a SolanaContext,
    value: EncodedAccountInfo<'a>,
}

#[derive(Debug)]
struct EncodedAccountData<'a> {
    encoding: Encoding,
    data: &'a AccountData,
    slice: Option<Slice>,
}

impl EncodedAccountData<'_> {
    fn slice(self, slice: Option<Slice>) -> Self {
        EncodedAccountData {
            encoding: self.encoding,
            data: self.data,
            slice,
        }
    }
}

impl<'a> Serialize for EncodedAccountData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};
        let mut seq = serializer.serialize_seq(Some(2))?;
        let data = if let Some(slice) = &self.slice {
            self.data
                .data
                .get(slice.offset..slice.offset + slice.length)
                .ok_or(Error::custom("bad slice"))?
        } else {
            &self.data.data[..]
        };
        match self.encoding {
            Encoding::Base58 => {
                seq.serialize_element(&bs58::encode(&data).into_string())?;
            }
            Encoding::Base64 => {
                seq.serialize_element(&base64::encode(&data))?;
            }
            Encoding::Base64Zstd => {
                seq.serialize_element(&base64::encode(
                    zstd::encode_all(std::io::Cursor::new(&data), 0)
                        .map_err(|_| Error::custom("can't compress"))?,
                ))?;
            }
        }
        seq.serialize_element(&self.encoding)?;
        seq.end()
    }
}

#[derive(Clone)]
pub(crate) struct State {
    pub map: Arc<DashMap<Pubkey, AccountContext>>,
    pub client: Client,
    pub tx: Addr<AccountUpdateManager>,
    pub rpc_url: String,
    pub slot: Arc<AtomicU64>,
    pub map_updated: Arc<Notify>,
    pub request_limit: Arc<Semaphore>,
}

impl State {
    fn get(&self, key: &Pubkey) -> Option<dashmap::mapref::one::Ref<'_, Pubkey, AccountContext>> {
        let tx = &self.tx;
        self.map.get(key).map(|v| {
            tx.do_send(AccountCommand::Reset(*key));
            v
        })
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Request<'a> {
    jsonrpc: &'a str,
    id: u64,
    method: &'a str,
    #[serde(borrow)]
    params: &'a RawValue,
}

#[derive(Deserialize, Serialize, Debug)]
struct RpcError<'a> {
    code: i64,
    message: &'a str,
}

#[derive(Deserialize, Serialize, Debug)]
struct ErrorResponse<'a> {
    jsonrpc: &'a str,
    error: RpcError<'a>,
    id: u64,
}

impl ErrorResponse<'static> {
    fn not_enough_arguments(id: u64) -> ErrorResponse<'static> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32602,
                message: "`params` should have at least 1 argument(s)",
            },
        }
    }
}

async fn get_account_info<'a>(
    req: Request<'a>,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error> {
    #[inline]
    fn account_response(
        req_id: u64,
        acc: &AccountInfo,
        ctx: &SolanaContext,
        encoding: Encoding,
        slice: Option<Slice>,
    ) -> HttpResponse {
        #[derive(Serialize)]
        struct Resp<'a> {
            jsonrpc: &'a str,
            result: EncodedAccountContext<'a>, //AccountContext,
            id: u64,
        }
        let resp = Resp {
            jsonrpc: "2.0",
            result: acc.encode(encoding, slice).with_context(&ctx),
            id: req_id,
        };

        HttpResponse::Ok()
            .content_type("application/json")
            .json(&resp)
    }

    #[derive(Deserialize, Debug)]
    struct Config<'a> {
        encoding: Encoding,
        commitment: Option<&'a str>,
        #[serde(rename = "dataSlice")]
        data_slice: Option<Slice>,
    }
    impl Default for Config<'static> {
        fn default() -> Self {
            Config {
                encoding: Encoding::Base58,
                commitment: None,
                data_slice: None,
            }
        }
    }

    let params: SmallVec<[&RawValue; 2]> = serde_json::from_str(req.params.get())?;
    if params.is_empty() {
        return Ok(HttpResponse::Ok()
            .content_type("application/json")
            .json(ErrorResponse::not_enough_arguments(req.id)));
    }
    let pubkey: Pubkey = serde_json::from_str(params[0].get())?;
    let config: Config = {
        if let Some(param) = params.get(1) {
            serde_json::from_str(param.get())?
        } else {
            Config::default()
        }
    };

    let mut cacheable_for_key = None;

    match app_state.get(&pubkey) {
        Some(data) => {
            let data = data.value();
            info!("cache hit for {}", pubkey);
            return Ok(account_response(
                req.id,
                &data.value,
                &data.context,
                config.encoding,
                config.data_slice,
            ));
        }
        None => {
            if config.data_slice.is_none() {
                cacheable_for_key = Some(pubkey);
            }
            app_state
                .tx
                .send(AccountCommand::Subscribe(pubkey))
                .await
                .unwrap();
        }
    }

    let client = &app_state.client;
    let limit = &app_state.request_limit;
    let wait_for_response = async {
        let mut retries = 10; // todo: proper backoff
        loop {
            retries -= 1;
            let _permit = limit.acquire().await;
            let mut resp = client.post(&app_state.rpc_url).send_json(&req).await?;
            let body = resp
                .body()
                .await
                .map_err(|_| awc::error::SendRequestError::Timeout); // todo
            match body {
                Ok(body) => break Ok(body),
                Err(_) => {
                    tokio::time::delay_for(std::time::Duration::from_millis(100)).await;
                    if retries == 0 {
                        break Err(awc::error::SendRequestError::Timeout);
                    }
                }
            }
        }
    };

    tokio::pin!(wait_for_response);

    let resp = loop {
        let notified = app_state.map_updated.notified();
        tokio::select! {
            body = &mut wait_for_response => {
                if let Ok(body) = body {
                    break body;
                } else {
                    return Ok(HttpResponse::GatewayTimeout().finish());
                }
            }
            _ = notified => {
                if let Some(pubkey) = cacheable_for_key {
                    match app_state.get(&pubkey) {
                        Some(data) => {
                            let data = data.value();
                            info!("got hit in map while waiting!");
                            return Ok(account_response(
                                req.id,
                                &data.value,
                                &data.context,
                                config.encoding,
                                config.data_slice,
                            ));
                        },
                        None => {},
                    }
                }
                continue;
            }
        }
    };

    if let Some(pubkey) = cacheable_for_key {
        #[derive(Deserialize)]
        struct Resp {
            result: AccountContext,
        }
        let info: Resp = serde_json::from_slice(&resp)?;
        app_state.map.insert(pubkey, info.result);
        app_state.map_updated.notify();
    }

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(resp))
}

pub(crate) async fn rpc_handler(
    body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error> {
    let req: Request = serde_json::from_slice(&body)?;

    match req.method.as_ref() {
        "getAccountInfo" => {
            return get_account_info(req, app_state).await;
        }
        _ => {}
    }

    let client = &app_state.client;
    let resp = client
        .post(&app_state.rpc_url)
        .send_json(&req)
        .await
        .unwrap();

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .streaming(resp))
}
