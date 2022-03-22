use std::borrow::Cow;

use actix_http::error::PayloadError;
use actix_web::web::Header;
use actix_web::{web, HttpResponse, ResponseError};
use backoff::backoff::Backoff;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use serde_json::value::RawValue;
use tracing::{error, warn};

use crate::metrics::rpc_metrics as metrics;
use crate::rpc::request::{GetAccountInfo, GetProgramAccounts};
use crate::rpc::response::identity_response;

use super::request::{Id, Request, XRequestId, X_REQUEST_ID_NAME};
use super::response::Error;
use super::state::State;
use super::{backoff_settings, config};

enum OneOrMany<'a> {
    One(Request<'a, RawValue>),
    Many(Vec<Request<'a, RawValue>>),
}

impl<'a> OneOrMany<'a> {
    pub fn iter(&self) -> impl Iterator<Item = &Request<'a, RawValue>> {
        use either::Either;
        match self {
            OneOrMany::One(req) => Either::Left(std::iter::once(req)),
            OneOrMany::Many(reqs) => Either::Right(reqs.iter()),
        }
    }
}

impl<'de> Deserialize<'de> for OneOrMany<'de> {
    fn deserialize<D>(deserializer: D) -> Result<OneOrMany<'de>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = OneOrMany<'de>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("[] or {}")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let des = serde::de::value::SeqAccessDeserializer::new(seq);
                Ok(OneOrMany::Many(serde::Deserialize::deserialize(des)?))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let des = serde::de::value::MapAccessDeserializer::new(map);
                Ok(OneOrMany::One(serde::Deserialize::deserialize(des)?))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

pub async fn rpc_handler(
    xrid: Header<XRequestId>,
    body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    let xrid = xrid.into_inner();

    let req: OneOrMany<'_> = match serde_json::from_slice(&body) {
        Ok(val) => val,
        Err(_) => {
            warn!(xrequestid=%xrid, "invalid request, couldn't parse");
            return Ok(Error::InvalidRequest(None, Some("Invalid request"), xrid).error_response());
        }
    };

    config::check_config_change(&app_state).await;

    // run WAF checks on all subquiries in request
    if let Some(waf) = &app_state.waf {
        let lua = &waf.lua;
        for r in req.iter() {
            let res = lua.scope(|scope| {
                lua.globals()
                    .set("request", scope.create_nonstatic_userdata(r)?)?;
                lua.load("require 'waf'.request(request)")
                    .eval::<(bool, String)>()
            });

            let (ok, error) = match res {
                Ok(tuple) => tuple,
                Err(e) => {
                    tracing::error!(%e, "Error occured during WAF rules evaluation");
                    return Ok(Error::Internal(
                        Some(r.id.clone()),
                        Cow::from("WAF internal error"),
                    )
                    .error_response());
                }
            };
            if !ok {
                warn!(%error, "Request was rejected due to WAF rule violation");
                metrics().waf_rejections.inc();
                return Ok(Error::WAFRejection(Some(r.id.clone()), error, xrid).error_response());
            }
        }
    }

    let mut id = Id::Null;

    // extra header for passthrough requests
    let mut request_header = None;
    // if request contains only one query, try to serve it from cache
    if let OneOrMany::One(req) = req {
        id = req.id.clone();

        if req.jsonrpc != "2.0" {
            return Ok(Error::InvalidRequest(Some(id), None, xrid).error_response());
        }

        macro_rules! observe {
            ($method:expr, $fut:expr) => {{
                metrics().request_types($method).inc();
                let timer = metrics()
                    .handler_time
                    .with_label_values(&[$method])
                    .start_timer();
                let resp = $fut.await;
                timer.observe_duration();
                Ok(resp.unwrap_or_else(|err| err.error_response()))
            }};
        }

        let arc_state = app_state.clone().into_inner();
        match req.method {
            "getAccountInfo" => {
                tracing::info!(method=%req.method, id=?req.id, "handling cacheable method");
                return observe!(
                    req.method,
                    arc_state.process_request::<GetAccountInfo>(req, xrid)
                );
            }
            "getProgramAccounts" => {
                tracing::info!(method=%req.method, id=?req.id, "handling cacheable method");
                return observe!(
                    req.method,
                    arc_state.process_request::<GetProgramAccounts>(req, xrid)
                );
            }
            "getIdentity" if app_state.identity.is_some() => {
                return Ok(identity_response(
                    req.id,
                    app_state
                        .identity
                        .as_ref()
                        .expect("no identity, shouldn't happen"),
                ));
            }
            method => {
                metrics().request_types(method).inc();
                tracing::info!(%method, id=?req.id, "handling passthrough method");
                request_header = Some(("X-Cache-Request-Method", method.to_string()));
            }
        }
    } else {
        metrics().batch_requests.inc();
    }

    let client = app_state.client.clone();
    let url = app_state.rpc_url.clone();
    let error = Error::Timeout(id, xrid.clone()).error_response();

    let xreqid = xrid.0.clone();
    let stream = stream_generator::generate_stream(move |mut stream| async move {
        let mut backoff = backoff_settings(crate::PASSTHROUGH_BACKOFF);
        let total = metrics().passthrough_total_time.start_timer();
        loop {
            let request_time = metrics().passthrough_request_time.start_timer();
            let mut request = client.post(&url).content_type("application/json");
            if let Some(header) = request_header.as_ref() {
                request = request.append_header(header.clone());
            }
            request = request.append_header((X_REQUEST_ID_NAME, xreqid.as_str()));
            let resp = request.send_body(body.clone()).await.map_err(|error| {
                error!(%error, id="error while streaming response for passthrough request");
                metrics().streaming_errors.inc();
                error
            });
            metrics()
                .backend_requests_count
                .with_label_values(&["passthrough"])
                .inc(); // count request attempts, even failed ones

            request_time.observe_duration();
            match resp {
                Ok(mut resp) => {
                    let forward_response_time =
                        metrics().passthrough_forward_response_time.start_timer();
                    while let Some(chunk) = resp.next().await {
                        stream.send(chunk).await;
                    }
                    forward_response_time.observe_duration();
                    break;
                }
                Err(err) => {
                    metrics().passthrough_errors.inc();
                    match backoff.next_backoff() {
                        Some(duration) => {
                            metrics().request_retries.inc();
                            tokio::time::sleep(duration).await;
                        }
                        None => {
                            let mut error_stream = error.into_body();
                            use actix_web::body::MessageBody;
                            warn!("passthrough request error, retries exceeded: {:?}", err);
                            while let Some(chunk) = futures_util::future::poll_fn(|cx| {
                                std::pin::Pin::new(&mut error_stream).poll_next(cx)
                            })
                            .await
                            {
                                stream
                                    .send(chunk.map_err(|_| PayloadError::Incomplete(None))) // should never error
                                    .await;
                            }
                            break;
                        }
                    }
                }
            }
        }
        total.observe_duration();
    });

    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .append_header(xrid.as_header_tuple())
        .streaming(Box::pin(stream)))
}

pub async fn bad_content_type_handler() -> HttpResponse {
    HttpResponse::UnsupportedMediaType()
        .body("Supplied content type is not allowed. Content-Type: application/json is required")
}

pub async fn metrics_handler(
    _body: Bytes,
    app_state: web::Data<State>,
) -> Result<HttpResponse, Error<'static>> {
    use prometheus::{Encoder, TextEncoder};

    let current_limits = app_state.config.load().request_limits;
    metrics()
        .max_permits
        .with_label_values(&["getAccountInfo"])
        .set(current_limits.account_info as i64);

    metrics()
        .max_permits
        .with_label_values(&["getProgramAccounts"])
        .set(current_limits.program_accounts as i64);

    metrics().app_version.set(0);
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    let families = prometheus::gather();
    let _ = encoder.encode(&families, &mut buffer);
    Ok(HttpResponse::Ok().content_type("text/plain").body(buffer))
}
