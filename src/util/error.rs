use failure::Context;
use failure::{Backtrace, Fail};
use std::fmt::{self, Display};
use std::sync::PoisonError;
use surf::Error;
#[derive(Debug)]
pub struct VelliError {
    inner: Context<VelliErrorType>,
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Fail)]
pub enum VelliErrorType {
    #[fail(display = "IOError")]
    IOError,
    #[fail(display = "SerdeError")]
    SerdeError,
    #[fail(display = "UnknownOperation")]
    UnknownOperation,
    #[fail(display = "KeyNotFound")]
    KeyNotFound,
    #[fail(display = "LockFailed")]
    LockFailed,
    #[fail(display = "InvalidArguments")]
    InvalidArguments,
    #[fail(display = "DecodeError")]
    DecodeError,
    #[fail(display = "PoisonError")]
    PoisonError,
    #[fail(display = "RecvError")]
    RecvError,
    #[fail(display = "SendError")]
    SendError,
    #[fail(display = "ConnectionError")]
    ConnectionError,
    #[fail(display = "SurfError")]
    SurfError,
    #[fail(display = "Other")]
    Other,
}

impl Fail for VelliError {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for VelliError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl VelliError {
    /// 获取错误类型
    pub fn kind(&self) -> VelliErrorType {
        *(self.inner.get_context())
    }
}

impl From<VelliErrorType> for VelliError {
    fn from(kind: VelliErrorType) -> VelliError {
        VelliError {
            inner: Context::new(kind),
        }
    }
}

impl From<Context<VelliErrorType>> for VelliError {
    fn from(inner: Context<VelliErrorType>) -> VelliError {
        VelliError { inner: inner }
    }
}

impl From<std::io::Error> for VelliError {
    fn from(_: std::io::Error) -> VelliError {
        VelliErrorType::IOError.into()
    }
}

impl<T> From<PoisonError<T>> for VelliError {
    fn from(_: PoisonError<T>) -> VelliError {
        VelliErrorType::PoisonError.into()
    }
}

impl From<std::sync::mpsc::RecvError> for VelliError {
    fn from(_: std::sync::mpsc::RecvError) -> Self {
        VelliErrorType::RecvError.into()
    }
}

impl From<async_std::channel::RecvError> for VelliError {
    fn from(_: async_std::channel::RecvError) -> Self {
        VelliErrorType::RecvError.into()
    }
}

impl<T> From<std::sync::mpsc::SendError<T>> for VelliError {
    fn from(_: std::sync::mpsc::SendError<T>) -> Self {
        VelliErrorType::SendError.into()
    }
}

impl<T> From<async_std::channel::SendError<T>> for VelliError {
    fn from(_: async_std::channel::SendError<T>) -> Self {
        VelliErrorType::SendError.into()
    }
}

impl From<surf::Error> for VelliError {
    fn from(_: surf::Error) -> Self {
        VelliErrorType::SurfError.into()
    }
}

pub type Result<T> = std::result::Result<T, VelliError>;
