//! Service configuration, loaded from environment variables.
//!
//! [`Config::from_lookup`] is the pure logic behind [`Config::from_env`], so
//! tests never touch the process environment.

use std::ffi::OsString;
use std::fmt;

use secrecy::SecretString;

const HTTP_HOST_VAR: &str = "HTTP_HOST";
const HTTP_PORT_VAR: &str = "HTTP_PORT";
const GATEWAY_SHARED_SECRET_VAR: &str = "GATEWAY_SHARED_SECRET";
const SECURITY_EVENTS_STREAM_NAME_VAR: &str = "SECURITY_EVENTS_STREAM_NAME";
const AWS_REGION_VAR: &str = "AWS_REGION";

/// Runtime configuration of `events-service`.
pub struct Config {
    /// Host the HTTP server binds to (`HTTP_HOST`).
    pub http_host: String,
    /// Port the HTTP server binds to (`HTTP_PORT`).
    pub http_port: u16,
    /// Shared secret the gateway sends on every request
    /// (`GATEWAY_SHARED_SECRET`). Redacted in `Debug`, never logged.
    pub gateway_shared_secret: SecretString,
    /// Kinesis stream to read (`SECURITY_EVENTS_STREAM_NAME`). Optional;
    /// used by the consumer in a later task.
    pub security_events_stream_name: Option<String>,
    /// AWS region (`AWS_REGION`). Optional; used by the consumer later.
    pub aws_region: Option<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("http_host", &self.http_host)
            .field("http_port", &self.http_port)
            .field("gateway_shared_secret", &"[REDACTED]")
            .field(
                "security_events_stream_name",
                &self.security_events_stream_name,
            )
            .field("aws_region", &self.aws_region)
            .finish()
    }
}

impl Config {
    /// Loads the configuration from the process environment.
    ///
    /// # Errors
    ///
    /// See [`Config::from_lookup`].
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    /// Loads the configuration through `lookup` (variable name to value, or
    /// `None` when unset). Required: `HTTP_HOST`, `HTTP_PORT`,
    /// `GATEWAY_SHARED_SECRET`. A blank required value counts as missing.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MissingVar`] for an absent, blank or non-UTF-8
    /// required variable; [`ConfigError::InvalidPort`] when `HTTP_PORT` is
    /// not a `u16`.
    pub fn from_lookup<F>(lookup: F) -> Result<Self, ConfigError>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let http_host = read_required(&lookup, HTTP_HOST_VAR)?;
        let raw_port = read_required(&lookup, HTTP_PORT_VAR)?;
        let gateway_shared_secret =
            SecretString::from(read_required(&lookup, GATEWAY_SHARED_SECRET_VAR)?);
        let http_port = raw_port
            .parse::<u16>()
            .map_err(|_| ConfigError::InvalidPort {
                variable: HTTP_PORT_VAR,
            })?;

        Ok(Config {
            http_host,
            http_port,
            gateway_shared_secret,
            security_events_stream_name: read_optional(&lookup, SECURITY_EVENTS_STREAM_NAME_VAR),
            aws_region: read_optional(&lookup, AWS_REGION_VAR),
        })
    }
}

fn read_optional(
    lookup: &impl Fn(&str) -> Option<OsString>,
    variable: &'static str,
) -> Option<String> {
    lookup(variable)
        .and_then(|value| value.into_string().ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_required(
    lookup: &impl Fn(&str) -> Option<OsString>,
    variable: &'static str,
) -> Result<String, ConfigError> {
    read_optional(lookup, variable).ok_or(ConfigError::MissingVar { variable })
}

/// Error returned when the configuration cannot be loaded. Variants carry
/// the variable name only, never its value.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    /// A required environment variable is absent, blank or not UTF-8.
    #[error("missing required environment variable: {variable}")]
    MissingVar {
        /// Name of the missing variable.
        variable: &'static str,
    },
    /// `HTTP_PORT` is not a valid TCP port.
    #[error("environment variable {variable} does not hold a valid TCP port")]
    InvalidPort {
        /// Name of the invalid variable.
        variable: &'static str,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    const SECRET_VALUE: &str = "super-secret-value-for-tests";

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, OsString> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), OsString::from(v)))
            .collect()
    }

    fn valid_env() -> HashMap<String, OsString> {
        env(&[
            (HTTP_HOST_VAR, "0.0.0.0"),
            (HTTP_PORT_VAR, "8080"),
            (GATEWAY_SHARED_SECRET_VAR, SECRET_VALUE),
        ])
    }

    fn load(map: &HashMap<String, OsString>) -> Result<Config, ConfigError> {
        Config::from_lookup(|name| map.get(name).cloned())
    }

    #[test]
    fn loads_valid_config_with_optional_vars_unset() {
        let config = load(&valid_env()).expect("valid config");
        assert_eq!(config.http_host, "0.0.0.0");
        assert_eq!(config.http_port, 8080);
        assert_eq!(config.security_events_stream_name, None);
        assert_eq!(config.aws_region, None);
    }

    #[test]
    fn loads_optional_vars_trimmed_and_ignores_blank_ones() {
        let mut map = valid_env();
        map.insert(
            SECURITY_EVENTS_STREAM_NAME_VAR.into(),
            " security-events ".into(),
        );
        map.insert(AWS_REGION_VAR.into(), "   ".into());
        let config = load(&map).expect("valid config");
        assert_eq!(
            config.security_events_stream_name.as_deref(),
            Some("security-events")
        );
        assert_eq!(config.aws_region, None);
    }

    #[test]
    fn fails_when_the_shared_secret_is_missing() {
        let mut map = valid_env();
        map.remove(GATEWAY_SHARED_SECRET_VAR);
        assert_eq!(
            load(&map).unwrap_err(),
            ConfigError::MissingVar {
                variable: GATEWAY_SHARED_SECRET_VAR
            }
        );
    }

    #[test]
    fn fails_when_the_shared_secret_is_blank() {
        let mut map = valid_env();
        map.insert(GATEWAY_SHARED_SECRET_VAR.into(), "   ".into());
        assert_eq!(
            load(&map).unwrap_err(),
            ConfigError::MissingVar {
                variable: GATEWAY_SHARED_SECRET_VAR
            }
        );
    }

    #[test]
    fn fails_when_the_port_is_not_a_valid_u16() {
        let mut map = valid_env();
        map.insert(HTTP_PORT_VAR.into(), "not-a-port".into());
        assert_eq!(
            load(&map).unwrap_err(),
            ConfigError::InvalidPort {
                variable: HTTP_PORT_VAR
            }
        );
    }

    #[test]
    fn debug_output_does_not_print_the_secret() {
        let config = load(&valid_env()).expect("valid config");
        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains(SECRET_VALUE),
            "secret leaked: {rendered}"
        );
    }
}
