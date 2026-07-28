//! Owner-authorized, restart-applied ACP configuration overrides.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::{ConfigError, RespondTo};

const MAX_PROMPT_BYTES: usize = 65_536;

/// The only settings that an owner may change through a signed DM.
///
/// Credentials, executable paths, environment variables, and OpenCode files
/// deliberately have no representation here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RemoteConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub respond_to: Option<RespondTo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub respond_to_allowlist: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channels: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_turn_duration_secs: Option<u64>,
}

impl RemoteConfig {
    /// Parse an exact `!config {json}` owner command and validate its bounded data.
    pub fn parse_command(content: &str) -> Result<Self, String> {
        let json = content
            .trim()
            .strip_prefix("!config ")
            .ok_or("expected '!config' followed by a JSON object")?;
        let config: Self = serde_json::from_str(json).map_err(|error| error.to_string())?;
        config.validate()?;
        Ok(config)
    }

    pub fn load(path: &Path) -> Result<Option<Self>, ConfigError> {
        match std::fs::read_to_string(path) {
            Ok(json) => {
                let config: Self = serde_json::from_str(&json).map_err(|error| {
                    ConfigError::ConfigFile(format!(
                        "invalid remote config {}: {error}",
                        path.display()
                    ))
                })?;
                config.validate().map_err(ConfigError::ConfigFile)?;
                Ok(Some(config))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Persist atomically so a systemd restart never observes partial JSON.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        self.validate()?;
        let parent = path
            .parent()
            .ok_or("remote config path has no parent directory")?;
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let temp = path.with_extension("tmp");
        std::fs::write(
            &temp,
            serde_json::to_vec_pretty(self).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
        }
        std::fs::rename(temp, path).map_err(|error| error.to_string())
    }

    fn validate(&self) -> Result<(), String> {
        if self
            .system_prompt
            .as_ref()
            .is_some_and(|value| value.len() > MAX_PROMPT_BYTES)
        {
            return Err(format!("system_prompt exceeds {MAX_PROMPT_BYTES} bytes"));
        }
        if self
            .model
            .as_ref()
            .is_some_and(|value| value.len() > 256 || value.trim().is_empty())
        {
            return Err("model must be 1 to 256 bytes".to_string());
        }
        if self.agents.is_some_and(|value| !(1..=32).contains(&value)) {
            return Err("agents must be between 1 and 32".to_string());
        }
        if self.idle_timeout_secs == Some(0) {
            return Err("idle_timeout_secs must be positive".to_string());
        }
        if self
            .max_turn_duration_secs
            .is_some_and(|value| !(60..=604_800).contains(&value))
        {
            return Err("max_turn_duration_secs must be between 60 and 604800".to_string());
        }
        if let (Some(idle), Some(max)) = (self.idle_timeout_secs, self.max_turn_duration_secs) {
            if idle >= max {
                return Err(
                    "idle_timeout_secs must be less than max_turn_duration_secs".to_string()
                );
            }
        }
        if self.respond_to != Some(RespondTo::Allowlist) && self.respond_to_allowlist.is_some() {
            return Err("respond_to_allowlist requires respond_to=allowlist".to_string());
        }
        if self.respond_to == Some(RespondTo::Allowlist)
            && self.respond_to_allowlist.as_ref().is_none_or(Vec::is_empty)
        {
            return Err(
                "respond_to=allowlist requires a non-empty respond_to_allowlist".to_string(),
            );
        }
        for channel in self.channels.as_deref().unwrap_or_default() {
            if channel.parse::<uuid::Uuid>().is_err() {
                return Err(format!("invalid channel UUID '{channel}'"));
            }
        }
        for pubkey in self.respond_to_allowlist.as_deref().unwrap_or_default() {
            if pubkey.len() != 64 || !pubkey.chars().all(|value| value.is_ascii_hexdigit()) {
                return Err(format!("invalid allowlist pubkey '{pubkey}'"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RemoteConfig;

    #[test]
    fn accepts_a_bounded_config_command() {
        let config = RemoteConfig::parse_command(
            r#"!config {"model":"gpt-5","respond_to":"owner-only","agents":2}"#,
        )
        .expect("valid command");
        assert_eq!(config.model.as_deref(), Some("gpt-5"));
    }

    #[test]
    fn rejects_unknown_and_unsafe_fields() {
        assert!(RemoteConfig::parse_command(r#"!config {"env_vars":{"X":"Y"}}"#).is_err());
        assert!(RemoteConfig::parse_command(r#"!config {"agent_command":"sh"}"#).is_err());
    }
}
