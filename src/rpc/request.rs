use actix_http::header::{Header, HeaderName, HeaderValue, InvalidHeaderValue, TryIntoHeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use smallvec::SmallVec;
use std::fmt::{self, Display};

use crate::filter::{Filter, Filters};
use crate::types::{AccountInfo, Commitment, Encoding, Pubkey, Slot, SolanaContext};

use super::response::Error;
use super::{hash, HasOwner, Slice};

#[derive(Deserialize, Serialize, Debug)]
#[serde(bound(deserialize = "&'a T: Deserialize<'de>"))]
pub(super) struct Request<'a, T>
where
    T: ?Sized,
{
    pub(super) jsonrpc: &'a str,
    pub(super) id: Id<'a>,
    pub(super) method: &'a str,
    #[serde(borrow)]
    pub(super) params: Option<&'a T>,
}

pub(super) struct GetAccountInfo {
    pub(super) pubkey: Pubkey,
    pub(super) config: AccountInfoConfig,
    pub(super) config_hash: u64,
}

#[derive(Clone)]
pub(super) struct GetProgramAccounts {
    pub(super) pubkey: Pubkey,
    pub(super) config: ProgramAccountsConfig,
    pub(super) filters: Option<Filters>,
    pub(super) valid_filters: bool,
}

#[derive(Debug, Default, Clone)]
pub struct XRequestId(pub String);

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum Id<'a> {
    Null,
    Num(u64),
    Str(&'a str),
}

#[derive(Debug)]
pub enum IdOwned {
    Null,
    Num(u64),
    Str(String),
}

#[derive(Deserialize, Debug)]
pub(super) struct AccountAndPubkey {
    pub(super) account: AccountInfo,
    pub(super) pubkey: Pubkey,
}

#[derive(Deserialize, Debug)]
pub(super) struct Flatten<T> {
    #[serde(flatten)]
    pub(super) inner: T,
}

#[derive(Deserialize, Debug)]
pub(super) struct AccountInfoConfig {
    #[serde(default = "Encoding::default")]
    pub(super) encoding: Encoding,
    #[serde(flatten)]
    pub(super) commitment: Option<CommitmentConfig>,
    #[serde(rename = "dataSlice")]
    pub(super) data_slice: Option<Slice>,
}

#[derive(Deserialize, Debug, Clone)]
pub(super) struct ProgramAccountsConfig {
    #[serde(default = "Encoding::default")]
    pub(super) encoding: Encoding,
    #[serde(flatten)]
    pub(super) commitment: Option<CommitmentConfig>,
    #[serde(rename = "dataSlice")]
    pub(super) data_slice: Option<Slice>,
    pub(super) filters: Option<MaybeFilters>,
    #[serde(rename = "withContext")]
    pub(super) with_context: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(from = "SmallVec<[Filter; 3]>")]
pub(super) enum MaybeFilters {
    Valid(Filters),
    Invalid(SmallVec<[Filter; 3]>),
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(super) enum MaybeContext<T> {
    With { context: SolanaContext, value: T },
    Without(T),
}

#[derive(Deserialize, Serialize, Debug, Copy, Clone)]
pub(super) struct CommitmentConfig {
    pub(super) commitment: Commitment,
}

impl Default for CommitmentConfig {
    fn default() -> Self {
        CommitmentConfig {
            commitment: Commitment::Finalized,
        }
    }
}

impl From<SmallVec<[Filter; 3]>> for MaybeFilters {
    fn from(value: SmallVec<[Filter; 3]>) -> MaybeFilters {
        Filters::new_normalized(value.clone()).map_or(Self::Invalid(value), Self::Valid)
    }
}

impl<T> MaybeContext<T> {
    pub fn into_slot_and_value(self) -> (Option<Slot>, T) {
        match self {
            Self::With { context, value } => (Some(context.slot), value),
            Self::Without(value) => (None, value),
        }
    }
}

impl Default for ProgramAccountsConfig {
    fn default() -> Self {
        ProgramAccountsConfig {
            encoding: Encoding::Default,
            commitment: None,
            data_slice: None,
            filters: None,
            with_context: None,
        }
    }
}

impl Default for AccountInfoConfig {
    fn default() -> Self {
        AccountInfoConfig {
            encoding: Encoding::Base58,
            commitment: None,
            data_slice: None,
        }
    }
}

impl Display for XRequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", X_REQUEST_ID_NAME, self.0)
    }
}

impl TryIntoHeaderValue for XRequestId {
    type Error = InvalidHeaderValue;
    fn try_into_value(self) -> Result<actix_http::header::HeaderValue, Self::Error> {
        HeaderValue::from_str(&self.0)
    }
}

pub const X_REQUEST_ID_NAME: &str = "X-Request-ID";
impl Header for XRequestId {
    fn name() -> HeaderName {
        HeaderName::from_static(X_REQUEST_ID_NAME)
    }

    fn parse<M: actix_http::HttpMessage>(msg: &M) -> Result<Self, actix_http::error::ParseError> {
        let v = msg.headers().get(X_REQUEST_ID_NAME);
        let id = v
            .and_then(|v| v.to_str().map(ToOwned::to_owned).ok())
            .unwrap_or_else(generate_request_id);
        Ok(Self(id))
    }
}

impl XRequestId {
    pub fn as_header_tuple(&self) -> (&'static str, &str) {
        (X_REQUEST_ID_NAME, self.0.as_str())
    }
}

impl<'a, 'b> mlua::UserData for &'b Request<'a, RawValue> {
    fn add_fields<'lua, F: mlua::UserDataFields<'lua, Self>>(fields: &mut F) {
        fields.add_field_method_get("jsonrpc", |_, this| Ok(this.jsonrpc));
        fields.add_field_method_get("method", |_, this| Ok(this.method));
        fields.add_field_method_get("params", |_, this| Ok(this.params.map(|v| v.get())));
    }
}

impl GetAccountInfo {
    pub fn commitment(&self) -> Commitment {
        self.config
            .commitment
            .map_or_else(Commitment::default, |commitment| commitment.commitment)
    }
}

impl fmt::Display for GetAccountInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "getAccountInfo {{ pubkey: {}, commitment: {:?} }}",
            self.pubkey,
            self.commitment()
        )
    }
}

impl GetProgramAccounts {
    pub fn commitment(&self) -> Commitment {
        self.config
            .commitment
            .map_or_else(Commitment::default, |commitment| commitment.commitment)
    }
}

impl HasOwner for MaybeContext<Vec<AccountAndPubkey>> {}

impl fmt::Display for GetProgramAccounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: print filters as well
        write!(
            f,
            "getProgramAccounts {{ pubkey: {}, commitment: {:?} }}",
            self.pubkey,
            self.commitment()
        )
    }
}

impl<'a> From<Id<'a>> for IdOwned {
    fn from(id: Id<'a>) -> Self {
        match id {
            Id::Null => IdOwned::Null,
            Id::Num(num) => IdOwned::Num(num),
            Id::Str(s) => IdOwned::Str(s.to_owned()),
        }
    }
}

pub(super) fn parse_params<'a, T: Default + Deserialize<'a>>(
    req: &Request<'a, RawValue>,
) -> Result<(Pubkey, T, u64), Error<'a>> {
    let (params, request_hash): (SmallVec<[&RawValue; 2]>, u64) = match req.params {
        Some(params) => (serde_json::from_str(params.get())?, hash(params.get())),
        None => return Err(Error::NotEnoughArguments(req.id.clone())),
    };

    if params.is_empty() {
        return Err(Error::NotEnoughArguments(req.id.clone()));
    }

    if params.len() > 2 {
        return Err(Error::InvalidParam {
            req_id: req.id.clone(),
            message: "Invalid parameters: Expected from 1 to 2 parameters".into(),
            data: Some(format!("\"Got {}\"", params.len()).into()),
        });
    }

    let pubkey: Pubkey = match serde_json::from_str(params[0].get()) {
        Ok(pubkey) => pubkey,
        Err(_) => {
            return Err(Error::InvalidParam {
                req_id: req.id.clone(),
                message: "Invalid param: WrongSize".into(),
                data: None,
            })
        }
    };

    let config: T = {
        if let Some(param) = params.get(1) {
            serde_json::from_str(param.get()).map_err(|err| Error::InvalidParam {
                req_id: req.id.clone(),
                message: format!("Invalid params: {}", err).into(),
                data: None,
            })?
        } else {
            T::default()
        }
    };

    Ok((pubkey, config, request_hash))
}

#[inline]
pub(super) fn generate_request_id() -> String {
    const HEX_DIGITS: &[u8; 22] = b"0123456789abcdefABCDEF";
    use rand::seq::SliceRandom;
    let mut rng = rand::thread_rng();

    (0..32)
        .map(|_| *HEX_DIGITS.choose(&mut rng).unwrap() as char)
        .collect()
}
