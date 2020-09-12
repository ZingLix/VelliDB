use failure::Context;
use failure::{Backtrace, Fail};
use std::fmt::{self, Display};
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

pub type Result<T> = std::result::Result<T, VelliError>;
