use anyhow::Result;
use axum::{
    extract::{Path, State},
    http::{header::LOCATION, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Deserialize)]
struct ShortnRequest {
    url: String,
}

#[derive(Debug, Serialize)]
struct ShortnResponse {
    id: String,
    url: String,
}

#[derive(Debug, Clone)]
struct AppState {
    db: PgPool,
}

#[allow(dead_code)]
#[derive(Debug, FromRow)]
struct UrlRecord {
    id: String,
    url: String,
}

#[derive(Debug, Error)]
enum ShortnError {
    #[error("Failed to connect to the database")]
    ConnectionFailure,
    #[error("Failed to execute the shortner query")]
    ShortnRequestError,
    #[error("Failed to get the url")]
    GetUrlError,
    #[error("Failed to serve")]
    ServeAbnormal,
}

#[tokio::main]
async fn main() -> Result<(), ShortnError> {
    // initialize tracing subscriber
    tracing_subscriber::fmt::init();

    let url = "postgres://ronny@localhost:5432/shortner";
    let state = AppState::try_new(url).await?;
    info!("Connected to the database {url}");

    let addr = "127.0.0.1:9876";
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|_| ShortnError::ConnectionFailure)?;
    info!("Listening on {}", addr);

    // initialize the router with a post request and a get request for id
    let router = Router::new()
        .route("/", post(shortner))
        .route("/:id", get(redirect))
        .with_state(state);

    let _ = axum::serve(listener, router.into_make_service())
        .await
        .map_err(|_| ShortnError::ConnectionFailure);

    Ok(())
}

async fn shortner(
    State(state): State<AppState>,
    data: Json<ShortnRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    let id = state
        .shortn(&data.url)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let body = ShortnResponse {
        url: format!("http://127.0.0.1:9876/{}", id),
        id,
    };

    Ok((StatusCode::CREATED, Json(body)))
}

async fn redirect(
    Path(id): Path<String>,
    State(state): State<AppState>,
) -> Result<impl IntoResponse, StatusCode> {
    let url = state.get_url(&id).await.map_err(|e| {
        info!("Failed to get url {:?}", e);
        StatusCode::NOT_FOUND
    })?;
    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, url.parse().unwrap());
    Ok((StatusCode::FOUND, headers))
}

impl AppState {
    async fn try_new(url: &str) -> Result<Self, ShortnError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|_| ShortnError::ConnectionFailure)?;
        // create the table if it does not exist
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS urls (
                id CHAR(6) PRIMARY KEY,
                url TEXT NOT NULL UNIQUE
            )
            "#,
        )
        .execute(&pool)
        .await
        .map_err(|_| ShortnError::ServeAbnormal)?;

        Ok(Self { db: pool })
    }

    async fn shortn(&self, url: &str) -> Result<String, ShortnError> {
        let id = nanoid!(6);
        let row: UrlRecord = sqlx::query_as(
            r#"
            INSERT INTO urls (id, url) VALUES ($1, $2) ON CONFLICT(url)
            DO UPDATE SET id=excluded.id, url=excluded.url
            RETURNING *
            "#,
        )
        .bind(&id)
        .bind(url)
        .fetch_one(&self.db)
        .await
        .map_err(|_| ShortnError::ShortnRequestError)?;

        Ok(row.id)
    }

    async fn get_url(&self, id: &str) -> Result<String, ShortnError> {
        let record: (String,) = sqlx::query_as(
            r#"
            SELECT url FROM urls WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(&self.db)
        .await
        .map_err(|_| ShortnError::GetUrlError)?;

        Ok(record.0)
    }
}
