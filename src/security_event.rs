//! Consumer-side view of the `security-event` contract
//! (`broker/contracts/security-event.schema.json`): the envelope and its
//! typed accessors. The producers (gateway, nmap-service, user-service) own
//! the constructors; this service only reads events, so the type has no
//! builders and is obtained from [`crate::event_contract::parse_record`].

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Type of a [`SecurityEvent`]; the 11 values of the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventType {
    /// A user requested a scan and the broker accepted it.
    #[serde(rename = "scan.requested")]
    ScanRequested,
    /// A user requested a scan cancellation and the broker accepted it.
    #[serde(rename = "scan.cancelled")]
    ScanCancelled,
    /// `nmap-service` started a scan.
    #[serde(rename = "scan.started")]
    ScanStarted,
    /// `nmap-service` finished a scan.
    #[serde(rename = "scan.completed")]
    ScanCompleted,
    /// `nmap-service` failed a scan.
    #[serde(rename = "scan.failed")]
    ScanFailed,
    /// `nmap-service` found a high or critical finding.
    #[serde(rename = "finding.critical")]
    FindingCritical,
    /// A user completed the login.
    #[serde(rename = "auth.login")]
    AuthLogin,
    /// A user logged out.
    #[serde(rename = "auth.logout")]
    AuthLogout,
    /// A request exceeded the rate limit.
    #[serde(rename = "ratelimit.hit")]
    RatelimitHit,
    /// A message was sent to a dead-letter queue.
    #[serde(rename = "message.deadlettered")]
    MessageDeadlettered,
    /// `user-service` recorded an audit action.
    #[serde(rename = "audit.recorded")]
    AuditRecorded,
}

impl EventType {
    /// Contract value (`scan.requested`, ...).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ScanRequested => "scan.requested",
            Self::ScanCancelled => "scan.cancelled",
            Self::ScanStarted => "scan.started",
            Self::ScanCompleted => "scan.completed",
            Self::ScanFailed => "scan.failed",
            Self::FindingCritical => "finding.critical",
            Self::AuthLogin => "auth.login",
            Self::AuthLogout => "auth.logout",
            Self::RatelimitHit => "ratelimit.hit",
            Self::MessageDeadlettered => "message.deadlettered",
            Self::AuditRecorded => "audit.recorded",
        }
    }
}

/// Service that produced a [`SecurityEvent`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventSource {
    /// The gateway.
    #[serde(rename = "gateway")]
    Gateway,
    /// `nmap-service`.
    #[serde(rename = "nmap-service")]
    NmapService,
    /// `user-service`.
    #[serde(rename = "user-service")]
    UserService,
}

/// Immutable security event: the envelope of the contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecurityEvent {
    event_id: String,
    event_type: EventType,
    occurred_at: String,
    source: EventSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    actor_sub: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scan_id: Option<String>,
    payload: Value,
}

impl SecurityEvent {
    /// Unique UUID of this event (consumers dedupe on it: Kinesis delivers at
    /// least once).
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// Event type.
    pub fn event_type(&self) -> EventType {
        self.event_type
    }

    /// Event type as the contract text.
    pub fn event_type_str(&self) -> &'static str {
        self.event_type.as_str()
    }

    /// Event instant, RFC 3339.
    pub fn occurred_at(&self) -> &str {
        &self.occurred_at
    }

    /// Service that produced the event.
    pub fn source(&self) -> EventSource {
        self.source
    }

    /// `sub` of the user that caused the event, when applicable.
    pub fn actor_sub(&self) -> Option<&str> {
        self.actor_sub.as_deref()
    }

    /// Scan the event belongs to, when applicable.
    pub fn scan_id(&self) -> Option<&str> {
        self.scan_id.as_deref()
    }

    /// Type-specific payload.
    pub fn payload(&self) -> &Value {
        &self.payload
    }
}
