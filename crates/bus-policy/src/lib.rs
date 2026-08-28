#![forbid(unsafe_code)]
#![warn(missing_docs)]
//! Versioned authorization policy for BUS/1.

use std::collections::BTreeMap;
use std::fmt;

use bus_protocol::{Channel, ClientId, Headers, Namespace, PeerId};

/// Broker-verified operating-system identity.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Credentials {
    /// Process identifier.
    pub pid: u32,
    /// User identifier.
    pub uid: u32,
    /// Primary group identifier.
    pub gid: u32,
    /// Kernel-resolved executable path when it is available.
    pub executable: Option<String>,
    /// Kernel security label when it is available.
    pub security_label: Option<String>,
    /// Kernel cgroup path when it is available.
    pub cgroup: Option<String>,
}

/// An action presented to a policy implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Action {
    /// Open a connection.
    Connect,
    /// Claim an API namespace.
    ClaimNamespace(Namespace),
    /// Subscribe to a multicast channel.
    Subscribe(Channel),
    /// Publish to a multicast channel.
    Publish(Channel),
    /// Send to a direct peer.
    SendPeer(PeerId),
    /// Send to a namespace provider.
    SendNamespace(Namespace),
    /// Send to peers selected by a client implementation identifier.
    SendClient(ClientId),
    /// Send a global broadcast.
    Broadcast,
    /// Acknowledge a delivered message.
    Acknowledge,
    /// Enter privileged monitor mode.
    Monitor,
    /// Register a future D-Bus name.
    RegisterDbusName(String),
}

impl Action {
    /// Returns the stable policy action name.
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::ClaimNamespace(_) => "claim-namespace",
            Self::Subscribe(_) => "subscribe",
            Self::Publish(_) => "publish",
            Self::SendPeer(_) => "send-peer",
            Self::SendNamespace(_) => "send-namespace",
            Self::SendClient(_) => "send-client",
            Self::Broadcast => "broadcast",
            Self::Acknowledge => "acknowledge",
            Self::Monitor => "monitor",
            Self::RegisterDbusName(_) => "register-dbus-name",
        }
    }

    fn target(&self) -> Option<String> {
        match self {
            Self::ClaimNamespace(value) | Self::SendNamespace(value) => Some(value.to_string()),
            Self::Subscribe(value) | Self::Publish(value) => Some(value.to_string()),
            Self::SendPeer(value) => Some(value.to_string()),
            Self::SendClient(value) => Some(value.to_string()),
            Self::RegisterDbusName(value) => Some(value.clone()),
            Self::Connect | Self::Broadcast | Self::Acknowledge | Self::Monitor => None,
        }
    }
}

/// Context used for one authorization decision.
#[derive(Clone, Debug)]
pub struct Request<'a> {
    /// The broker-authenticated process identity.
    pub credentials: &'a Credentials,
    /// The client-provided implementation identifier, if any.
    pub client_id: Option<&'a ClientId>,
    /// The client-provided metadata, kept separate from authenticated metadata.
    pub claimed_headers: &'a Headers,
    /// The requested operation.
    pub action: &'a Action,
}

/// Decides whether an authenticated peer may perform an action.
pub trait Policy: Send + Sync {
    /// Returns whether the request is permitted.
    fn permits(&self, request: &Request<'_>) -> bool;
}

/// A permissive policy suitable only for tests and explicitly unsafe development use.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

impl Policy for AllowAll {
    fn permits(&self, _: &Request<'_>) -> bool {
        true
    }
}

impl<T: Policy + ?Sized> Policy for Box<T> {
    fn permits(&self, request: &Request<'_>) -> bool {
        (**self).permits(request)
    }
}

/// The production-safe policy used when no policy file is configured.
///
/// It permits ordinary local messaging while keeping privileged operations
/// (`broadcast`, monitoring, and D-Bus registration) deny-by-default.
#[derive(Clone, Copy, Debug, Default)]
pub struct SafeDefaults;

impl Policy for SafeDefaults {
    fn permits(&self, request: &Request<'_>) -> bool {
        !matches!(
            request.action,
            Action::Broadcast | Action::Monitor | Action::RegisterDbusName(_)
        )
    }
}

/// A policy rule effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Effect {
    /// Permit a matching request.
    Allow,
    /// Reject a matching request.
    Deny,
}

/// A parsed, versioned policy configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    /// Configuration format version. Only version 1 is currently supported.
    pub version: u8,
    /// Decision used when no rule matches.
    pub default: Effect,
    /// Rules evaluated in file order.
    pub rules: Vec<Rule>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            default: Effect::Deny,
            rules: Vec::new(),
        }
    }
}

/// One ordered authorization rule.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Rule {
    /// Decision made when every populated selector matches.
    pub effect: Option<Effect>,
    /// Stable action names. An empty list matches every action.
    pub actions: Vec<String>,
    /// Required authenticated user ID.
    pub uid: Option<u32>,
    /// Required authenticated primary group ID.
    pub gid: Option<u32>,
    /// Required executable path.
    pub executable: Option<String>,
    /// Required security label.
    pub security_label: Option<String>,
    /// Required cgroup path.
    pub cgroup: Option<String>,
    /// Required claimed client identifier.
    pub client_id: Option<String>,
    /// Required action target or prefix ending in `*`.
    pub target: Option<String>,
    /// Required claimed textual header values.
    pub headers: BTreeMap<String, String>,
}

/// A policy that evaluates a parsed [`Config`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigPolicy {
    config: Config,
}

impl ConfigPolicy {
    /// Validates and constructs a policy from a parsed configuration.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        validate_config(&config)?;
        Ok(Self { config })
    }

    /// Parses a version 1 policy file and constructs a policy.
    pub fn parse(input: &str) -> Result<Self, ConfigError> {
        Self::new(Config::parse(input)?)
    }

    /// Returns the validated source configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl Policy for ConfigPolicy {
    fn permits(&self, request: &Request<'_>) -> bool {
        self.config
            .rules
            .iter()
            .find(|rule| rule_matches(rule, request))
            .and_then(|rule| rule.effect)
            .unwrap_or(self.config.default)
            == Effect::Allow
    }
}

impl Config {
    /// Parses the documented line-oriented `busd-policy/1` format.
    pub fn parse(input: &str) -> Result<Self, ConfigError> {
        let mut config = Self::default();
        let mut rule = None;
        for (index, raw_line) in input.lines().enumerate() {
            let line_number = index + 1;
            let line = raw_line
                .split_once('#')
                .map_or(raw_line, |(line, _)| line)
                .trim();
            if line.is_empty() {
                continue;
            }
            if line == "[[rule]]" {
                if let Some(rule) = rule.take() {
                    config.rules.push(rule);
                }
                rule = Some(Rule::default());
                continue;
            }
            let Some((key, raw_value)) = line.split_once('=') else {
                return Err(ConfigError::new(line_number, "expected key = value"));
            };
            let key = key.trim();
            let value = unquote(raw_value.trim(), line_number)?;
            let destination = rule.as_mut();
            match (destination, key) {
                (None, "version") => {
                    config.version = value
                        .parse()
                        .map_err(|_| ConfigError::new(line_number, "version must be an integer"))?;
                }
                (None, "default") => config.default = parse_effect(&value, line_number)?,
                (Some(rule), "effect") => rule.effect = Some(parse_effect(&value, line_number)?),
                (Some(rule), "actions") => {
                    rule.actions = value
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(str::to_owned)
                        .collect();
                }
                (Some(rule), "uid") => rule.uid = Some(parse_number(&value, line_number, "uid")?),
                (Some(rule), "gid") => rule.gid = Some(parse_number(&value, line_number, "gid")?),
                (Some(rule), "executable") => rule.executable = Some(value),
                (Some(rule), "security-label") => rule.security_label = Some(value),
                (Some(rule), "cgroup") => rule.cgroup = Some(value),
                (Some(rule), "client-id") => rule.client_id = Some(value),
                (Some(rule), "target") => rule.target = Some(value),
                (Some(rule), key) if key.starts_with("header.") => {
                    let name = key.trim_start_matches("header.");
                    if name.is_empty() {
                        return Err(ConfigError::new(
                            line_number,
                            "header selector needs a name",
                        ));
                    }
                    rule.headers.insert(name.into(), value);
                }
                (None, _) => return Err(ConfigError::new(line_number, "unknown top-level key")),
                (Some(_), _) => return Err(ConfigError::new(line_number, "unknown rule key")),
            }
        }
        if let Some(rule) = rule {
            config.rules.push(rule);
        }
        validate_config(&config)?;
        Ok(config)
    }
}

/// A policy-file parsing or validation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    line: usize,
    message: String,
}

impl ConfigError {
    fn new(line: usize, message: impl Into<String>) -> Self {
        Self {
            line,
            message: message.into(),
        }
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.line == 0 {
            self.message.fmt(formatter)
        } else {
            write!(formatter, "policy line {}: {}", self.line, self.message)
        }
    }
}

impl std::error::Error for ConfigError {}

fn validate_config(config: &Config) -> Result<(), ConfigError> {
    if config.version != 1 {
        return Err(ConfigError::new(0, "unsupported policy version"));
    }
    for rule in &config.rules {
        if rule.effect.is_none() {
            return Err(ConfigError::new(0, "every rule requires an effect"));
        }
        for action in &rule.actions {
            if !matches!(
                action.as_str(),
                "connect"
                    | "claim-namespace"
                    | "subscribe"
                    | "publish"
                    | "send-peer"
                    | "send-namespace"
                    | "send-client"
                    | "broadcast"
                    | "acknowledge"
                    | "monitor"
                    | "register-dbus-name"
            ) {
                return Err(ConfigError::new(0, "unknown action"));
            }
        }
    }
    Ok(())
}

fn rule_matches(rule: &Rule, request: &Request<'_>) -> bool {
    let credentials = request.credentials;
    (rule.actions.is_empty() || rule.actions.iter().any(|action| action == request.action.name()))
        && rule.uid.is_none_or(|uid| uid == credentials.uid)
        && rule.gid.is_none_or(|gid| gid == credentials.gid)
        && rule.executable.as_ref().is_none_or(|value| credentials.executable.as_ref() == Some(value))
        && rule.security_label.as_ref().is_none_or(|value| credentials.security_label.as_ref() == Some(value))
        && rule.cgroup.as_ref().is_none_or(|value| credentials.cgroup.as_ref() == Some(value))
        && rule.client_id.as_ref().is_none_or(|value| request.client_id.is_some_and(|client_id| client_id.as_str() == value))
        && rule.target.as_ref().is_none_or(|selector| target_matches(selector, request.action.target().as_deref()))
        && rule.headers.iter().all(|(name, value)| matches!(request.claimed_headers.get(name), Some(bus_protocol::HeaderValue::Text(actual)) if actual == value))
}

fn target_matches(selector: &str, target: Option<&str>) -> bool {
    let Some(target) = target else {
        return false;
    };
    selector
        .strip_suffix('*')
        .map_or(selector == target, |prefix| target.starts_with(prefix))
}

fn parse_effect(value: &str, line: usize) -> Result<Effect, ConfigError> {
    match value {
        "allow" => Ok(Effect::Allow),
        "deny" => Ok(Effect::Deny),
        _ => Err(ConfigError::new(line, "effect must be allow or deny")),
    }
}

fn parse_number(value: &str, line: usize, name: &str) -> Result<u32, ConfigError> {
    value
        .parse()
        .map_err(|_| ConfigError::new(line, format!("{name} must be an unsigned integer")))
}

fn unquote(value: &str, line: usize) -> Result<String, ConfigError> {
    if value.starts_with('"') || value.ends_with('"') {
        if value.len() < 2 || !value.starts_with('"') || !value.ends_with('"') {
            return Err(ConfigError::new(
                line,
                "strings must use matching double quotes",
            ));
        }
        return Ok(value[1..value.len() - 1].into());
    }
    Ok(value.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bus_protocol::HeaderValue;

    fn request<'a>(
        credentials: &'a Credentials,
        client_id: &'a ClientId,
        action: &'a Action,
        headers: &'a Headers,
    ) -> Request<'a> {
        Request {
            credentials,
            client_id: Some(client_id),
            claimed_headers: headers,
            action,
        }
    }

    #[test]
    fn policy_is_default_deny_and_uses_first_match() {
        let policy = ConfigPolicy::parse(
            "version = 1\ndefault = deny\n\n[[rule]]\neffect = deny\nactions = claim-namespace\ntarget = bus://system*\n\n[[rule]]\neffect = allow\nactions = claim-namespace\nuid = 0\n",
        ).unwrap();
        let action = Action::ClaimNamespace(Namespace::parse("bus://systemd").unwrap());
        let root = Credentials {
            uid: 0,
            ..Credentials::default()
        };
        let client_id = ClientId::parse("worker").unwrap();
        assert!(!policy.permits(&request(&root, &client_id, &action, &Headers::new())));
        let allowed = Action::ClaimNamespace(Namespace::parse("bus://user-service").unwrap());
        assert!(policy.permits(&request(&root, &client_id, &allowed, &Headers::new())));
    }

    #[test]
    fn policy_can_match_authenticated_and_claimed_selectors_separately() {
        let policy = ConfigPolicy::parse(
            "version = 1\ndefault = deny\n[[rule]]\neffect = allow\nactions = publish\nuid = 1000\nexecutable = /usr/bin/worker\nsecurity-label = unconfined\ncgroup = /user.slice\nclient-id = worker\nheader.role = producer\ntarget = events.*\n",
        ).unwrap();
        let credentials = Credentials {
            uid: 1000,
            executable: Some("/usr/bin/worker".into()),
            security_label: Some("unconfined".into()),
            cgroup: Some("/user.slice".into()),
            ..Credentials::default()
        };
        let action = Action::Publish(Channel::parse("events.changed").unwrap());
        let headers = [("role".into(), HeaderValue::Text("producer".into()))].into();
        let client_id = ClientId::parse("worker").unwrap();
        assert!(policy.permits(&request(&credentials, &client_id, &action, &headers)));
        let forged = Credentials {
            uid: 1,
            ..credentials.clone()
        };
        assert!(!policy.permits(&request(&forged, &client_id, &action, &headers)));
    }

    #[test]
    fn invalid_configuration_is_rejected() {
        assert!(ConfigPolicy::parse("version = 2").is_err());
        assert!(
            ConfigPolicy::parse("version = 1\n[[rule]]\neffect = allow\nactions = unknown")
                .is_err()
        );
        assert!(ConfigPolicy::parse("version = 1\nunknown = value").is_err());
    }
}
