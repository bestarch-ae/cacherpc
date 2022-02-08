use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use actix_web::{HttpResponse, ResponseError};
use dashmap::mapref::one::Ref;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use thiserror::Error as ThisError;
use tracing::{debug, warn};

use crate::filter::Filters;
use crate::metrics::rpc_metrics as metrics;
use crate::rpc::request::MaybeContext;
use crate::rpc::Slice;
use crate::types::{
    AccountData, AccountInfo, AccountState, Commitment, Encoding, Pubkey, Slot, SolanaContext,
};

use super::{hash, LruEntry};
use super::{
    request::{AccountInfoConfig, Id, ProgramAccountsConfig},
    state::State,
    HasOwner,
};

#[cfg(feature = "jsonparsed")]
use solana_sdk::pubkey::Pubkey as SolanaPubkey;

pub(super) struct CachedResponse {
    pub(super) owner: Option<Pubkey>,
    pub(super) response: HttpResponse,
}

#[derive(Serialize)]
struct JsonRpcResponse<'a, T> {
    jsonrpc: &'a str,
    result: T,
    id: Id<'a>,
}

#[derive(ThisError, Debug)]
#[error("can't encode in base58")]
struct Base58Error;

#[derive(Debug, ThisError)]
pub enum Error<'a> {
    #[error("invalid request")]
    InvalidRequest(Option<Id<'a>>, Option<&'a str>),
    #[error("waf rejection error")]
    WAFRejection(Option<Id<'a>>, String),
    #[error("invalid param")]
    InvalidParam {
        req_id: Id<'a>,
        message: Cow<'a, str>,
        data: Option<Cow<'a, str>>,
    },
    #[error("parsing request")]
    Parsing(Option<Id<'a>>),
    #[error("not enough arguments")]
    NotEnoughArguments(Id<'a>),
    #[error("backend timeout")]
    Timeout(Id<'a>),
    #[error("forward error")]
    Forward(#[from] awc::error::SendRequestError),
    #[error("streaming error")]
    Streaming(awc::error::PayloadError),
    #[error("internal error")]
    Internal(Option<Id<'a>>, Cow<'a, str>),
}

#[derive(Deserialize, Serialize, Debug)]
struct ErrorResponse<'a> {
    id: Option<Id<'a>>,
    jsonrpc: &'a str,
    error: RpcError<'a>,
}

#[derive(ThisError, Debug)]
pub(super) enum ProgramAccountsResponseError {
    #[error("serialization failed")]
    Serialize(#[from] serde_json::Error),
    #[error("data inconsistency")]
    Inconsistency,
    #[error("base58")]
    Base58,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub(super) enum Response<T> {
    Result(T),
    Error(RpcErrorOwned),
}

#[derive(Deserialize, Serialize, Debug)]
struct RpcError<'a> {
    code: i64,
    #[serde(borrow)]
    message: Cow<'a, str>,
    #[serde(borrow)]
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Cow<'a, RawValue>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub(super) struct RpcErrorOwned {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

impl From<serde_json::Error> for Error<'_> {
    fn from(_err: serde_json::Error) -> Self {
        Error::Parsing(None)
    }
}

impl From<awc::error::PayloadError> for Error<'_> {
    fn from(err: awc::error::PayloadError) -> Self {
        Error::Streaming(err)
    }
}

#[derive(Serialize, Debug)]
struct EncodedAccountContext<'a> {
    context: &'a SolanaContext,
    value: Option<EncodedAccountInfo<'a>>,
}

impl<'a> EncodedAccountContext<'a> {
    fn empty(ctx: &'a SolanaContext) -> EncodedAccountContext<'_> {
        EncodedAccountContext {
            context: ctx,
            value: None,
        }
    }
}

#[derive(Debug)]
struct EncodedAccountInfo<'a> {
    encoding: Encoding,
    slice: Option<Slice>,
    account_info: &'a AccountInfo,
    pubkey: Pubkey,
}

struct EncodedAccountData<'a> {
    encoding: Encoding,
    data: &'a AccountData,
    slice: Option<Slice>,
    #[cfg(feature = "jsonparsed")]
    pubkey: Pubkey,
    #[cfg(feature = "jsonparsed")]
    owner: Pubkey,
}

impl<'a> EncodedAccountInfo<'a> {
    pub(super) fn with_context(self, ctx: &'a SolanaContext) -> EncodedAccountContext<'a> {
        EncodedAccountContext {
            value: Some(self),
            context: ctx,
        }
    }
}

impl<'a> Serialize for EncodedAccountInfo<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut account_info = serializer.serialize_struct("AccountInfo", 5)?;
        let encoded_data = EncodedAccountData {
            encoding: self.encoding,
            data: &self.account_info.data,
            slice: self.slice,
            #[cfg(feature = "jsonparsed")]
            owner: self.account_info.owner,
            #[cfg(feature = "jsonparsed")]
            pubkey: self.pubkey,
        };
        account_info.serialize_field("data", &encoded_data)?;
        account_info.serialize_field("lamports", &self.account_info.lamports)?;
        account_info.serialize_field("owner", &self.account_info.owner)?;
        account_info.serialize_field("executable", &self.account_info.executable)?;
        account_info.serialize_field("rentEpoch", &self.account_info.rent_epoch)?;
        account_info.end()
    }
}

impl AccountInfo {
    fn encode(
        &self,
        encoding: Encoding,
        slice: Option<Slice>,
        enforce_base58_limit: bool,
        pubkey: Pubkey,
    ) -> Result<EncodedAccountInfo<'_>, Base58Error> {
        // Encoded binary (base 58) data should be less than 128 bytes
        if enforce_base58_limit && self.data.len() > 128 && encoding.is_base58() {
            return Err(Base58Error);
        }
        Ok(EncodedAccountInfo {
            account_info: self,
            slice,
            encoding,
            pubkey,
        })
    }
}

impl<'a> Serialize for EncodedAccountData<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error, SerializeSeq};

        let encoding = self.encoding;

        #[cfg(feature = "jsonparsed")]
        if let Encoding::JsonParsed = self.encoding {
            use solana_account_decoder::{
                parse_account_data::{parse_account_data, AccountAdditionalData},
                parse_token::get_token_account_mint,
            };

            let pubkey = SolanaPubkey::new_from_array(self.pubkey.into());
            let program_id = SolanaPubkey::new_from_array(self.owner.into());

            let additional_data = get_token_account_mint(&self.data.data)
                .map(|key| get_mint_decimals(&key).ok())
                .map(|decimals| AccountAdditionalData {
                    spl_token_decimals: decimals,
                });

            match parse_account_data(&pubkey, &program_id, &self.data.data, additional_data) {
                Ok(parsed_acc) => {
                    return parsed_acc.serialize(serializer);
                }
                Err(_err) => {
                    // if we failed to parse the data, try to pass the request on to validator
                    return Err(Error::custom("Couldn't parse cached data"));
                }
            }
        }

        let data = if let Some(slice) = &self.slice {
            let end = slice
                .offset
                .saturating_add(slice.length)
                .min(self.data.data.len());
            self.data.data.get(slice.offset..end).unwrap_or(&[])
        } else {
            &self.data.data[..]
        };

        if let Encoding::Default = self.encoding {
            return serializer.serialize_str(&bs58::encode(&data).into_string());
        }

        let mut seq = serializer.serialize_seq(Some(2))?;

        match encoding {
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
            Encoding::JsonParsed => {
                #[cfg_attr(feature = "jsonparsed", allow(unused))]
                return Err(Error::custom("jsonParsed encoding is not supported"));
            }
            Encoding::Default => {
                panic!("default encoding should've been handled before");
            }
        }
        seq.serialize_element(&encoding)?;
        seq.end()
    }
}

impl ResponseError for Error<'_> {
    fn error_response(&self) -> HttpResponse {
        match self {
            Error::InvalidRequest(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::invalid_request(req_id.clone(), *msg)),
            Error::WAFRejection(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::waf_rejection(req_id.clone(), msg)),
            Error::InvalidParam {
                req_id,
                message,
                data,
            } => HttpResponse::Ok().content_type("application/json").json(
                &ErrorResponse::invalid_param(req_id.clone(), message.clone(), data.clone()),
            ),
            Error::Internal(req_id, msg) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::internal(req_id.clone(), msg.clone())),
            Error::Parsing(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::parse_error(req_id.clone())),
            Error::NotEnoughArguments(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::not_enough_arguments(req_id.clone())),
            Error::Timeout(req_id) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(Some(req_id.clone()))),
            Error::Forward(_) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(None)),
            Error::Streaming(_) => HttpResponse::Ok()
                .content_type("application/json")
                .json(&ErrorResponse::gateway_timeout(None)),
        }
    }
}

impl<'a> ErrorResponse<'a> {
    fn not_enough_arguments(id: Id<'a>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id: Some(id),
            error: RpcError {
                code: -32602,
                message: "`params` should have at least 1 argument(s)".into(),
                data: None,
            },
        }
    }

    fn invalid_param(
        id: Id<'a>,
        msg: Cow<'a, str>,
        data: Option<Cow<'a, str>>,
    ) -> ErrorResponse<'a> {
        let data = data
            .and_then(|data| serde_json::value::to_raw_value(&data).ok())
            .map(Cow::Owned);
        ErrorResponse {
            jsonrpc: "2.0",
            id: Some(id),
            error: RpcError {
                code: -32602,
                message: msg,
                data,
            },
        }
    }

    fn internal(id: Option<Id<'a>>, msg: Cow<'a, str>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32603,
                message: msg,
                data: None,
            },
        }
    }

    fn waf_rejection(id: Option<Id<'a>>, msg: &'a str) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -33000,
                message: Cow::from(msg),
                data: None,
            },
        }
    }

    fn invalid_request(id: Option<Id<'a>>, msg: Option<&'a str>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32600,
                message: Cow::from(msg.unwrap_or("Invalid request")),
                data: None,
            },
        }
    }

    fn parse_error(id: Option<Id<'a>>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32700,
                message: "Parse error".into(),
                data: None,
            },
        }
    }

    fn gateway_timeout(id: Option<Id<'a>>) -> ErrorResponse<'a> {
        ErrorResponse {
            jsonrpc: "2.0",
            id,
            error: RpcError {
                code: -32000,
                message: "Gateway timeout".into(),
                data: None,
            },
        }
    }
}

impl<'a> HasOwner for Result<CachedResponse, Error<'a>> {
    fn owner(&self) -> Option<Pubkey> {
        self.as_ref().ok().map(|data| data.owner).flatten()
    }
}

pub(super) fn identity_response<'a, 'b>(req_id: Id<'a>, identity: &'b str) -> HttpResponse {
    let mut map = HashMap::new();
    map.insert("identity", identity);
    let resp = JsonRpcResponse {
        jsonrpc: "2.0",
        result: map,
        id: req_id,
    };

    let body = serde_json::to_vec(&resp).expect("couldn't serialize identity");

    return HttpResponse::Ok()
        .append_header(("x-cache-status", "hit"))
        .append_header(("x-cache-type", "lru"))
        .content_type("application/json")
        .body(body);
}

pub(super) fn account_response<'a, 'b>(
    req_id: Id<'a>,
    request_hash: u64,
    acc: (Option<&'b AccountInfo>, u64),
    app_state: &State,
    config: &AccountInfoConfig,
    pubkey: Pubkey,
) -> Result<HttpResponse, Error<'a>> {
    let request_and_slot_hash = hash((request_hash, acc.1));
    if let Some(result) = app_state.lru.borrow_mut().get(&request_and_slot_hash) {
        metrics().lru_cache_hits.inc();
        let resp = JsonRpcResponse {
            jsonrpc: "2.0",
            result: result.as_ref(),
            id: req_id,
        };

        let body = serde_json::to_vec(&resp)?;

        metrics()
            .response_size_bytes
            .with_label_values(&["getAccountInfo"])
            .observe(body.len() as f64);

        return Ok(HttpResponse::Ok()
            .append_header(("x-cache-status", "hit"))
            .append_header(("x-cache-type", "lru"))
            .content_type("application/json")
            .body(body));
    }

    let slot = acc.1;
    let ctx = SolanaContext { slot };
    let result = acc
            .0
            .as_ref()
            .map(|acc| {
                metrics()
                    .account_data_len
                    .with_label_values(&[&app_state.worker_id])
                    .observe(acc.data.len() as f64);
                Ok::<_, Base58Error>(
                    acc.encode(config.encoding, config.data_slice, true, pubkey)?
                        .with_context(&ctx),
                )
            })
            .transpose()
            .map_err(|_| Error::InvalidRequest(Some(req_id.clone()),
                    Some("Encoded binary (base 58) data should be less than 128 bytes, please use Base64 encoding.")))?
            .unwrap_or_else(|| EncodedAccountContext::empty(&ctx));
    let timer = metrics()
        .serialization_time
        .with_label_values(&[
            "getAccountInfo",
            config.encoding.as_str(),
            &app_state.worker_id,
        ])
        .start_timer();
    let result = serde_json::value::to_raw_value(&result)?;
    let resp = JsonRpcResponse {
        jsonrpc: "2.0",
        result: &result,
        id: req_id,
    };
    let body = serde_json::to_vec(&resp)?;
    timer.observe_duration();
    app_state
        .lru
        .borrow_mut()
        .put(request_and_slot_hash, LruEntry::from(result));

    metrics()
        .lru_cache_filled
        .with_label_values(&[&app_state.worker_id])
        .set(app_state.lru.borrow().len() as i64);

    metrics()
        .response_size_bytes
        .with_label_values(&["getAccountInfo"])
        .observe(body.len() as f64);

    Ok(HttpResponse::Ok()
        .append_header(("x-cache-status", "hit"))
        .append_header(("x-cache-type", "data"))
        .content_type("application/json")
        .body(body))
}

pub(super) fn program_accounts_response<'a>(
    req_id: Id<'a>,
    accounts: &HashSet<Arc<Pubkey>>,
    config: &'_ ProgramAccountsConfig,
    filters: Option<&'a Filters>,
    app_state: &State,
    context: Option<Slot>,
) -> Result<HttpResponse, ProgramAccountsResponseError> {
    struct Encode<'a, K> {
        inner: Ref<'a, K, AccountState>,
        encoding: Encoding,
        slice: Option<Slice>,
        commitment: Commitment,
        enforce_base58_limit: bool,
        pubkey: Pubkey,
    }

    impl<'a, K> Serialize for Encode<'a, K>
    where
        K: Eq + std::hash::Hash,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            if let Some((Some(value), _)) = self.inner.value().get(self.commitment) {
                let encoded = value
                    .encode(
                        self.encoding,
                        self.slice,
                        self.enforce_base58_limit,
                        self.pubkey,
                    )
                    .map_err(serde::ser::Error::custom)?;
                encoded.serialize(serializer)
            } else {
                // shouldn't happen
                serializer.serialize_none()
            }
        }
    }

    #[derive(Serialize)]
    struct AccountAndPubkey<'a> {
        account: Encode<'a, Pubkey>,
        pubkey: Pubkey,
    }

    let commitment = config.commitment.unwrap_or_default().commitment;

    let mut encoded_accounts = Vec::with_capacity(accounts.len());
    let enforce_base58_limit = !app_state.config.load().ignore_base58_limit;

    for key in accounts {
        if let Some(data) = app_state.accounts.get(key) {
            let (account_info, _) = match data.value().get(commitment) {
                Some(data) => data,
                None => {
                    warn!("data for key {}/{:?} not found", key, commitment);
                    return Err(ProgramAccountsResponseError::Inconsistency);
                }
            };

            let account_len = account_info
                .as_ref()
                .map(|info| info.data.len())
                .unwrap_or(0);

            // TODO: kinda hacky, find a better way
            if enforce_base58_limit && account_len > 128 && config.encoding.is_base58() {
                return Err(ProgramAccountsResponseError::Base58);
            }

            if let Some(filters) = &filters {
                let matches = account_info.map_or(false, |acc| filters.matches(&acc.data));

                if !matches {
                    debug!(pubkey = ?data.key(), "skipped because of filter");
                    continue;
                }
            }
            metrics()
                .account_data_len
                .with_label_values(&[&app_state.worker_id])
                .observe(account_len as f64);

            encoded_accounts.push(AccountAndPubkey {
                account: Encode {
                    inner: data,
                    encoding: config.encoding,
                    slice: config.data_slice,
                    commitment,
                    enforce_base58_limit,
                    pubkey: **key,
                },
                pubkey: **key,
            })
        } else {
            warn!(key = %key, request_id = ?req_id, "data for key not found");
            return Err(ProgramAccountsResponseError::Inconsistency);
        }
    }

    let value = if let Some(slot) = context {
        MaybeContext::With {
            context: SolanaContext { slot },
            value: encoded_accounts,
        }
    } else {
        MaybeContext::Without(encoded_accounts)
    };

    let resp = JsonRpcResponse {
        jsonrpc: "2.0",
        result: value,
        id: req_id,
    };

    let timer = metrics()
        .serialization_time
        .with_label_values(&[
            "getProgramAccounts",
            config.encoding.as_str(),
            &app_state.worker_id,
        ])
        .start_timer();
    let body = serde_json::to_vec(&resp)?;
    timer.observe_duration();
    metrics()
        .response_size_bytes
        .with_label_values(&["getProgramAccounts"])
        .observe(body.len() as f64);
    Ok(HttpResponse::Ok()
        .append_header(("x-cache-status", "hit"))
        .append_header(("x-cache-type", "data"))
        .content_type("application/json")
        .body(body))
}

#[cfg(feature = "jsonparsed")]
fn get_mint_decimals(mint: &SolanaPubkey) -> Result<u8, &'static str> {
    use solana_account_decoder::parse_token::spl_token_native_mint;

    if mint == &spl_token_native_mint() {
        Ok(spl_token_v2_0::native_mint::DECIMALS)
    } else {
        Err("Invalid param: mint is not native")
    }
}
