use std::time::Instant;

#[cfg(feature = "aeron")]
use std::{ffi::CString, net::SocketAddr, time::Duration};

#[cfg(feature = "aeron")]
use anyhow::{Context, bail};
#[cfg(feature = "aeron")]
use rusteron_client::{
    Aeron, AeronAsyncAddExclusivePublication, AeronCError, AeronContext, AeronExclusivePublication,
    AeronSubscription, Handlers,
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
    ) -> Result<AeronPublication, AeronCError> {
        let channel = CString::new(channel.to_channel_string()).expect("invalid Aeron channel");
        let publication = self
            .aeron
            .async_add_exclusive_publication(&channel, stream_id)?
            .poll_blocking(Duration::from_secs(5))?;
        Ok(AeronPublication { publication })
    }

    pub fn begin_publisher(
        &self,
        channel: &AeronChannel,
        stream_id: i32,
    ) -> Result<AeronPublicationRegistration, AeronCError> {
        let channel = CString::new(channel.to_channel_string()).expect("invalid Aeron channel");
        let registration = self
            .aeron
            .async_add_exclusive_publication(&channel, stream_id)?;
        Ok(AeronPublicationRegistration { registration })
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

    pub fn ephemeral_udp_subscriber(
        &self,
        stream_id: i32,
    ) -> anyhow::Result<(AeronSubscriber, u16)> {
        let subscriber = self
            .subscriber(&AeronChannel::udp("0.0.0.0", 0), stream_id)
            .context("failed to register wildcard Aeron UDP subscription")?;
        let port = subscriber.resolved_udp_port()?;
        Ok((subscriber, port))
    }
}

#[cfg(feature = "aeron")]
pub struct AeronPublicationRegistration {
    registration: AeronAsyncAddExclusivePublication,
}

#[cfg(feature = "aeron")]
impl AeronPublicationRegistration {
    pub fn poll(&self) -> Result<Option<AeronPublication>, AeronCError> {
        self.registration
            .poll()
            .map(|publication| publication.map(|publication| AeronPublication { publication }))
    }
}

#[cfg(feature = "aeron")]
pub struct AeronPublication {
    publication: AeronExclusivePublication,
}

#[cfg(feature = "aeron")]
impl AeronPublication {
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

    /// Resolve the OS-assigned port of a wildcard Aeron UDP subscription.
    ///
    /// The subscription must have been registered with an endpoint such as
    /// `0.0.0.0:0`. Aeron fills the wildcard port after the media driver has
    /// created the receive endpoint.
    pub fn resolved_udp_port(&self) -> anyhow::Result<u16> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let uri = self
                .subscription
                .try_resolve_channel_endpoint_port_as_string(256)
                .context("failed to resolve Aeron UDP subscription endpoint")?;
            if let Some(port) = resolved_endpoint_port(&uri)? {
                return Ok(port);
            }
            if std::time::Instant::now() >= deadline {
                bail!("Aeron UDP subscription endpoint still has a wildcard port after 5 seconds");
            }
            std::thread::yield_now();
        }
    }
}

#[cfg(feature = "aeron")]
fn resolved_endpoint_port(channel_uri: &str) -> anyhow::Result<Option<u16>> {
    let endpoint = channel_uri
        .split_once('?')
        .map(|(_, params)| params)
        .unwrap_or(channel_uri)
        .split('|')
        .find_map(|parameter| parameter.strip_prefix("endpoint="))
        .context("resolved Aeron channel has no endpoint parameter")?;
    let address: SocketAddr = endpoint
        .parse()
        .with_context(|| format!("invalid resolved Aeron UDP endpoint {endpoint}"))?;
    Ok((address.port() != 0).then_some(address.port()))
}

#[cfg(all(test, feature = "aeron"))]
mod tests {
    use super::resolved_endpoint_port;

    #[test]
    fn extracts_resolved_wildcard_subscription_port() {
        assert_eq!(
            resolved_endpoint_port("aeron:udp?endpoint=0.0.0.0:40123").unwrap(),
            Some(40123)
        );
        assert_eq!(
            resolved_endpoint_port("aeron:udp?endpoint=0.0.0.0:0").unwrap(),
            None
        );
    }
}
