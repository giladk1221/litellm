from typing import Final

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ClaudeForwardProxySettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    host: str = "127.0.0.1"
    port: int = Field(default=4440, ge=1, le=65535)
    certificate: str
    private_key: str
    listener_certificate: str | None = None
    listener_private_key: str | None = None
    tunnel_hosts: tuple[str, ...] = (
        "claude.ai",
        ".claude.ai",
        "claude.com",
        ".claude.com",
        "anthropic.com",
        ".anthropic.com",
        ".claudeusercontent.com",
        ".claudemcpcontent.com",
        "storage.googleapis.com",
        "registry.npmjs.org",
        "github.com",
        "raw.githubusercontent.com",
        "http-intake.logs.us5.datadoghq.com",
        "browser-intake-us5-datadoghq.com",
        "formulae.brew.sh",
    )
    max_body_bytes: int = Field(default=32 * 1024 * 1024, ge=1)
    idle_timeout_seconds: float = Field(default=120, gt=0)
    max_connections: int = Field(default=256, ge=1)

    @model_validator(mode="after")
    def validate_listener(self) -> "ClaudeForwardProxySettings":
        paired: Final = (self.listener_certificate is None) == (self.listener_private_key is None)
        if not paired:
            raise ValueError("Both listener_certificate and listener_private_key are required for TLS")
        if self.host not in ("127.0.0.1", "::1", "localhost") and self.listener_certificate is None:
            raise ValueError("A non-loopback forward proxy requires listener TLS")
        if any(not host or "/" in host or ":" in host or "*" in host for host in self.tunnel_hosts):
            raise ValueError("tunnel_hosts must contain DNS names or dot-prefixed DNS suffixes")
        return self

    def permits_tunnel(self, hostname: str) -> bool:
        return any(hostname.endswith(host) if host.startswith(".") else hostname == host for host in self.tunnel_hosts)
