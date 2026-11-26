from typing import Final

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ClaudeForwardProxySettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = Field(default=4440, ge=1, le=65535)
    certificate: str | None = None
    private_key: str | None = None
    listener_certificate: str | None = None
    listener_private_key: str | None = None
    tunnel_hosts: tuple[str, ...] = (
        "claude.ai",
        ".claude.ai",
        "claude.com",
        ".claude.com",
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
    connection_lifetime_seconds: float = Field(default=3600, gt=0)
    shutdown_timeout_seconds: float = Field(default=30, ge=0)
    service_paths: tuple[str, ...] = (
        "/api/oauth/",
        "/api/claude_code/",
        "/api/claude_cli/",
        "/api/claude_code_penguin_mode",
        "/api/eval/",
        "/api/organizations/",
        "/api/event_logging/",
        "/mcp-registry/v0/servers",
        "/v1/mcp_servers",
        "/v1/mcp_servers/",
        "/v1/environments",
        "/v1/environments/",
        "/v1/sessions",
        "/v1/sessions/",
        "/v1/code/sessions",
        "/v1/code/sessions/",
    )

    @model_validator(mode="after")
    def validate_listener(self) -> "ClaudeForwardProxySettings":
        if not self.enabled:
            return self
        if not self.certificate or not self.private_key:
            raise ValueError("An enabled Claude forward proxy requires its inspection certificate and private key")
        paired: Final = (self.listener_certificate is None) == (self.listener_private_key is None)
        if not paired:
            raise ValueError("Both listener_certificate and listener_private_key are required for TLS")
        if self.host not in ("127.0.0.1", "::1", "localhost") and self.listener_certificate is None:
            raise ValueError("A non-loopback forward proxy requires listener TLS")
        if any(
            not host
            or host != host.lower()
            or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789.-" for char in host)
            or host in (".", "anthropic.com", ".anthropic.com", "api.anthropic.com")
            or host.endswith(".")
            or ".." in host
            for host in self.tunnel_hosts
        ):
            raise ValueError("tunnel_hosts must contain DNS names or dot-prefixed DNS suffixes")
        if any(
            not path.startswith("/")
            or not path.isascii()
            or len(path.strip("/").split("/")) < 2
            or any(char in path for char in ("%", "?", "#", "\\", "*"))
            or any(part in (".", "..") for part in path.split("/"))
            or "//" in path
            for path in self.service_paths
        ):
            raise ValueError("service_paths must contain specific origin paths or slash-terminated service prefixes")
        return self

    def permits_tunnel(self, hostname: str) -> bool:
        return any(hostname.endswith(host) if host.startswith(".") else hostname == host for host in self.tunnel_hosts)

    def permits_service(self, path: bytes) -> bool:
        return any(
            path.startswith(allowed.encode("ascii")) if allowed.endswith("/") else path == allowed.encode("ascii")
            for allowed in self.service_paths
        )
