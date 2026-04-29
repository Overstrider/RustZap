use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

tokio::task_local! {
    static REQUEST_ID: String;
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    IdempotencyConflict(String),
    #[error("{0}")]
    NotSupported(String),
    #[error("{0}")]
    RateLimited(String),
    #[error("{0}")]
    PayloadTooLarge(String),
    #[error("{0}")]
    ProviderError(String),
    #[error("{0}")]
    Internal(String),
}

#[derive(Debug, Serialize)]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    pub details: Value,
    pub request_id: String,
}

impl ApiError {
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) | Self::IdempotencyConflict(_) => StatusCode::CONFLICT,
            Self::NotSupported(_) => StatusCode::NOT_IMPLEMENTED,
            Self::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ProviderError(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::BadRequest(_) => "bad_request",
            Self::Unauthorized(_) => "unauthorized",
            Self::Forbidden(_) => "forbidden",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::IdempotencyConflict(_) => "idempotency_conflict",
            Self::NotSupported(_) => "not_supported",
            Self::RateLimited(_) => "rate_limited",
            Self::PayloadTooLarge(_) => "payload_too_large",
            Self::ProviderError(_) => "provider_error",
            Self::Internal(_) => "internal_error",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = self.status_code();
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code(),
                message: self.to_string(),
                details: json!({}),
                request_id: current_request_id(),
            },
        };
        (status, Json(body)).into_response()
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

pub fn new_request_id() -> String {
    format!("req_{}", Uuid::now_v7().simple())
}

pub fn current_request_id() -> String {
    REQUEST_ID
        .try_with(Clone::clone)
        .unwrap_or_else(|_| new_request_id())
}

pub async fn scope_request_id<F>(request_id: String, future: F) -> F::Output
where
    F: std::future::Future,
{
    REQUEST_ID.scope(request_id, future).await
}
