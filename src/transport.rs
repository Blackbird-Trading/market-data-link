//! Shared low-level data-plane primitives.
//!
//! Most users should start with [`crate::Client`] or [`crate::Server`].
//! Services with a busy-spin Aeron consumer can take an [`AeronSubscriber`]
//! from a connected client and access the underlying subscription here.

use std::time::Instant;

use std::{ffi::CString, net::SocketAddr, time::Duration};

use anyhow::{Context, bail};
use rusteron_client::{
    Aeron, AeronAsyncAddExclusivePublication, AeronCError, AeronContext, AeronExclusivePublication,
    AeronSubscription, Handlers,
};

use crate::protocol::AeronChannel;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Reliable control-plane endpoint.
pub enum ControlEndpoint {
    Tcp(String),
}

/// Default Aeron media-driver directory used by all link roles.
pub const AERON_DRIVER_DIR: &str = "/dev/shm/aeron";

#[derive(Debug, Clone)]
/// Raw data-plane payload plus the process-local receive timestamp.
pub struct InboundFrame {
    pub bytes: Vec<u8>,
    pub received_at: Instant,
}

/// Connection to the local Aeron media driver.
pub struct AeronClient {
    aeron: Aeron,
}

impl AeronClient {
    /// Connects to the media driver at [`AERON_DRIVER_DIR`].
    pub fn connect() -> Result<Self, AeronCError> {
        let context = AeronContext::new()?;
        let directory =
            CString::new(AERON_DRIVER_DIR).expect("Aeron directory contains a NUL byte");
        context.set_dir(&directory)?;
        let aeron = Aeron::new(&context)?;
        aeron.start()?;
        Ok(Self { aeron })
    }

    /// Registers an exclusive publication and waits until it is usable.
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

    /// Starts non-blocking exclusive-publication registration.
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

    /// Registers a subscription and waits until it is usable.
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

    /// Registers a wildcard UDP subscription and returns its resolved port.
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

/// An in-progress exclusive-publication registration.
pub struct AeronPublicationRegistration {
    registration: AeronAsyncAddExclusivePublication,
}

impl AeronPublicationRegistration {
    /// Returns the publication when registration completes.
    pub fn poll(&self) -> Result<Option<AeronPublication>, AeronCError> {
        self.registration
            .poll()
            .map(|publication| publication.map(|publication| AeronPublication { publication }))
    }
}

/// Ready exclusive Aeron publication.
pub struct AeronPublication {
    publication: AeronExclusivePublication,
}

impl AeronPublication {
    /// Offers one payload without retrying.
    pub fn offer(&self, bytes: &[u8]) -> i64 {
        self.publication
            .offer(bytes, Handlers::no_reserved_value_supplier_handler())
    }

    /// Returns whether the publication currently has a connected image.
    pub fn is_connected(&self) -> bool {
        self.publication.is_connected()
    }
}

/// Ready Aeron subscription owned by the consuming service loop.
pub struct AeronSubscriber {
    subscription: AeronSubscription,
}

impl AeronSubscriber {
    /// Exposes the low-level subscription for service-specific polling.
    pub fn subscription(&self) -> &AeronSubscription {
        &self.subscription
    }

    /// Returns whether the subscription currently has a connected image.
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

#[cfg(test)]
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
