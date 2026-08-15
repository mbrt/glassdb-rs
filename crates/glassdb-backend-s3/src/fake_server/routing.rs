//! HTTP routing and S3 response construction.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};

use super::parsing::{decode_page_token, encode_page_token, header_str, query_params, xml_escape};
use super::state::{
    DeleteObject, FakeState, HeadObject, ListedObjects, PutObject, PutRequest, ReadObject,
    StoredObject,
};

pub(super) async fn handle(
    state: Arc<FakeState>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    // SlowDown injection (port of the Go SlowDownTransport).
    if state.faults.take_slowdown(req.method()) {
        return Ok(slow_down());
    }

    let (parts, body) = req.into_parts();
    let method = parts.method.clone();
    let path = parts.uri.path().to_string();
    let query = parts.uri.query().unwrap_or("").to_string();
    let body = body
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .unwrap_or_default();

    let trimmed = path.trim_start_matches('/');
    let key = match trimmed.split_once('/') {
        Some((_bucket, key)) => key.to_string(),
        None => String::new(),
    };

    let is_list = key.is_empty() && method == Method::GET && query.contains("list-type=2");

    // Simulate the operation's wire latency before serving, so the client's
    // connection pool sees realistic in-flight times.
    if let Some(model) = &state.latency {
        model.sleep_for(&method, is_list).await;
    }

    let response = if key.is_empty() {
        // Bucket-level request.
        if is_list {
            list_objects(&state, &query)
        } else {
            // CreateBucket and anything else: accept.
            ok_empty()
        }
    } else {
        match method {
            Method::GET => get_object(&state, &key, &parts.headers),
            Method::HEAD => head_object(&state, &key),
            Method::PUT => put_object(&state, &key, &parts.headers, body.to_vec()),
            Method::DELETE => delete_object(&state, &key, &parts.headers),
            _ => xml_error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed", "nope"),
        }
    };
    Ok(response)
}

fn get_object(state: &FakeState, key: &str, headers: &hyper::HeaderMap) -> Response<Full<Bytes>> {
    match state.objects.get(
        key,
        || header_str(headers, "if-none-match"),
        || xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "key not found"),
        |etag| {
            Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", etag)
                .body(Full::new(Bytes::new()))
                .unwrap()
        },
        |object| object_response(object, true),
    ) {
        ReadObject::Missing(response)
        | ReadObject::NotModified(response)
        | ReadObject::Found(response) => response,
    }
}

fn head_object(state: &FakeState, key: &str) -> Response<Full<Bytes>> {
    match state.objects.head(
        key,
        || {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::new()))
                .unwrap()
        },
        |object| object_response(object, false),
    ) {
        HeadObject::Missing(response) | HeadObject::Found(response) => response,
    }
}

fn put_object(
    state: &FakeState,
    key: &str,
    headers: &hyper::HeaderMap,
    body: Vec<u8>,
) -> Response<Full<Bytes>> {
    let if_match = header_str(headers, "if-match");
    let if_none_match = header_str(headers, "if-none-match");

    let mut meta = HashMap::new();
    for (name, value) in headers {
        if let Some(key) = name.as_str().strip_prefix("x-amz-meta-")
            && let Ok(value) = value.to_str()
        {
            meta.insert(key.to_string(), value.to_string());
        }
    }

    let etag = match state.objects.put(
        PutRequest {
            key,
            body,
            meta,
            if_match: if_match.as_deref(),
            if_none_match: if_none_match.as_deref(),
        },
        || {
            xml_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "object exists",
            )
        },
        || {
            xml_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "etag mismatch",
            )
        },
    ) {
        PutObject::AlreadyExists(response) | PutObject::EtagMismatch(response) => return response,
        PutObject::Applied(etag) => etag,
    };

    // Lost-ack injection: the write above is durable, but the client is told the
    // request failed (500), so it cannot know the write landed.
    if state.faults.take_lost_ack() {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "we encountered an internal error",
        );
    }

    Response::builder()
        .status(StatusCode::OK)
        .header("ETag", etag)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn delete_object(
    state: &FakeState,
    key: &str,
    headers: &hyper::HeaderMap,
) -> Response<Full<Bytes>> {
    let if_match = header_str(headers, "if-match");
    match state.objects.delete(
        key,
        if_match.as_deref(),
        || xml_error(StatusCode::NOT_FOUND, "NoSuchKey", "object not found"),
        || {
            xml_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "etag mismatch",
            )
        },
    ) {
        DeleteObject::Missing(response) | DeleteObject::EtagMismatch(response) => return response,
        DeleteObject::Deleted => {}
    }

    if state.faults.take_lost_ack() {
        return xml_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            "we encountered an internal error",
        );
    }

    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn list_objects(state: &FakeState, query: &str) -> Response<Full<Bytes>> {
    let params = query_params(query);
    let prefix = params.get("prefix").cloned().unwrap_or_default();
    let delimiter = params.get("delimiter").cloned().unwrap_or_default();
    let max_keys = params
        .get("max-keys")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1000);
    let after = match params.get("continuation-token") {
        Some(token) => match decode_page_token(&prefix, token) {
            Some(after) => Some(after),
            None => {
                return xml_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgument",
                    "invalid continuation token",
                );
            }
        },
        None => None,
    };

    state.objects.list(
        &prefix,
        &delimiter,
        max_keys,
        after,
        |ListedObjects {
             contents,
             common,
             truncated,
         }| {
            let next = truncated
                .then(|| contents.last())
                .flatten()
                .map(|last| encode_page_token(&prefix, last));

            let mut xml = String::from(
                r#"<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
            );
            xml.push_str(&format!("<Name>test</Name><Prefix>{}</Prefix><MaxKeys>{max_keys}</MaxKeys><Delimiter>{}</Delimiter><IsTruncated>{truncated}</IsTruncated>", xml_escape(&prefix), xml_escape(&delimiter)));
            if let Some(next) = next {
                xml.push_str(&format!(
                    "<NextContinuationToken>{}</NextContinuationToken>",
                    xml_escape(&next)
                ));
            }
            for key in &contents {
                xml.push_str(&format!(
                    "<Contents><Key>{}</Key></Contents>",
                    xml_escape(key)
                ));
            }
            for prefix in &common {
                xml.push_str(&format!(
                    "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
                    xml_escape(prefix)
                ));
            }
            xml.push_str("</ListBucketResult>");

            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/xml")
                .body(Full::new(Bytes::from(xml)))
                .unwrap()
        },
    )
}

fn object_response(object: &StoredObject, with_body: bool) -> Response<Full<Bytes>> {
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", object.etag());
    for (key, value) in object.meta() {
        response = response.header(format!("x-amz-meta-{key}"), value);
    }
    let body = if with_body {
        Bytes::copy_from_slice(object.body())
    } else {
        response = response.header("content-length", object.body().len().to_string());
        Bytes::new()
    };
    response.body(Full::new(body)).unwrap()
}

fn slow_down() -> Response<Full<Bytes>> {
    xml_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "SlowDown",
        "Please reduce your request rate.",
    )
}

fn xml_error(status: StatusCode, code: &str, message: &str) -> Response<Full<Bytes>> {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><Error><Code>{code}</Code><Message>{message}</Message></Error>"#
    );
    Response::builder()
        .status(status)
        .header("content-type", "application/xml")
        .body(Full::new(Bytes::from(xml)))
        .unwrap()
}

fn ok_empty() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .body(Full::new(Bytes::new()))
        .unwrap()
}
