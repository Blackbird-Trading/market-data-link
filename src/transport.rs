use std::time::Instant;

#[cfg(feature = "aeron")]
use std::{ffi::CString, time::Duration};

#[cfg(feature = "aeron")]
use rusteron_client::{
    Aeron, AeronCError, AeronContext, AeronExclusivePublication, AeronSubscription, Handlers,
};

#[cfg(feature = "aeron")]
use crate::protocol::AeronChannel;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlEndpoint {
    Tcp(String),
}

pub const AERON_DRIVER_DIR: &str = "/dev/shm/aeron";

#[derive(Debug, Clone)]
pub struct InboundFrame {
    pub bytes: Vec<u8>,
    pub received_at: Instant,
}

#[cfg(feature = "aeron")]
pub struct AeronClient {
    aeron: Aeron,
}

#[cfg(feature = "aeron")]
impl AeronClient {
    pub fn connect() -> Result<Self, AeronCError> {
        let context = AeronContext::new()?;
        let directory =
            CString::new(AERON_DRIVER_DIR).expect("Aeron directory contains a NUL byte");
        context.set_dir(&directory)?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;
        Ok(Self { aeron })
    }

    pub fn publisher(
        &self,
        channel: &AeronChannel,
        stream_id: i32,
    ) -> Result<AeronPublisher, AeronCError> {
        let channel = CString::new(channel.to_channel_string()).expect("invalid Aeron channel");
        let publication = self
            .aeron
            .async_add_exclusive_publication(&channel, stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        Ok(AeronPublisher { publication })
    }

    pub fn subscriber(
        &self,
        channel: &AeronChannel,
        stream_id: i32,
    ) -> Result<AeronSubscriber, AeronCError> {
        let channel = CString::new(channel.to_channel_string()).expect("invalid Aeron channel");
        let subscription = self
            .aeron
            .async_add_subscription(
                &channel,
                stream_id,
                Handlers::no_available_image_handler(),
                Handlers::no_unavailable_image_handler(),
            )?
            .poll_blocking(Duration::from_secs(5))?;
        Ok(AeronSubscriber { subscription })
    }
}

#[cfg(feature = "aeron")]
pub struct AeronPublisher {
    publication: AeronExclusivePublication,
}

#[cfg(feature = "aeron")]
impl AeronPublisher {
    pub fn offer(&self, bytes: &[u8]) -> i64 {
        self.publication
            .offer(bytes, Handlers::no_reserved_value_supplier_handler())
    }

    pub fn is_connected(&self) -> bool {
        self.publication.is_connected()
    }
}

#[cfg(feature = "aeron")]
pub struct AeronSubscriber {
    subscription: AeronSubscription,
}

#[cfg(feature = "aeron")]
impl AeronSubscriber {
    pub fn subscription(&self) -> &AeronSubscription {
        &self.subscription
    }

    pub fn is_connected(&self) -> bool {
        self.subscription.is_connected()
    }
}
