use thiserror::Error;

#[derive(Debug, Error)]
pub enum PgWireError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server error: {0}")]
    Server(String),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("tls error: {0}")]
    Tls(String),

    #[error("task ended unexpectedly: {0}")]
    Task(String),
}

pub type Result<T> = std::result::Result<T, PgWireError>;
