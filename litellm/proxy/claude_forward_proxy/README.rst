Claude subscription forward proxy
=================================

This opt-in listener lets Claude Code keep its native subscription sign-in
and API hostname while LiteLLM processes local Messages API requests. It
runs inside the Python proxy workers. There is no client bridge process.
This is an experimental integration, not a certification of every Claude
feature or future client release.

This adds a transport to the existing Anthropic provider, not a new model provider.
It does not turn Bedrock, Vertex or Foundry credentials into a Claude subscription.
Managed skills and MCP keep their existing LiteLLM configuration and authentication.

Transport and authentication
----------------------------

Claude Code connects with HTTPS CONNECT using a LiteLLM virtual key in
proxy authentication. The listener terminates the inner TLS connection only
for api.anthropic.com. POST /v1/messages and /v1/messages/count_tokens enter the existing LiteLLM ASGI
application, including its authentication, routing, policies and accounting.
The user's original subscription OAuth header remains the provider credential.
Configure direct anthropic/* deployments and scope virtual keys accordingly.
Keep fallback deployments on direct Anthropic too. A request-scoped policy rejects
other providers, alternate API origins and configured deployment credentials.
It disables LiteLLM response caching for these subscription requests. Anthropic
prompt caching remains part of the native request.

Approved service paths on that host retain their native request bodies and credentials,
including feature-service WebSocket upgrades. Other allowed HTTPS hosts use
opaque CONNECT tunnels. LiteLLM credentials are not sent to upstream services.
Claude Code remains responsible for storing and refreshing its own sign-in.

Configuration
-------------

Python 3.11 or later is required when the listener is enabled. Leave
claude_forward_proxy absent or enabled false to retain the existing startup behavior.
An authenticated gateway with a configured LiteLLM master key is required.
The LITELLM_RUST_FIRST server topology is rejected at startup until that runtime
owns the listener lifecycle. This restriction does not disable Rust SDK inference.

Add this configuration to the existing proxy configuration::

    model_list:
      - model_name: anthropic/*
        litellm_params:
          model: anthropic/*
    general_settings:
      claude_forward_proxy:
        enabled: true
        host: 0.0.0.0
        port: 4440
        certificate: /etc/litellm/tls/anthropic-inspection.pem
        private_key: /etc/litellm/tls/anthropic-inspection.key
        listener_certificate: /etc/litellm/tls/proxy-server.pem
        listener_private_key: /etc/litellm/tls/proxy-server.key
        connection_lifetime_seconds: 3600

The inspection certificate must cover api.anthropic.com and chain to a CA
trusted by the Claude Code process. The listener certificate must cover the
gateway's actual hostname. Use an enterprise-managed certificate lifecycle.
The listener needs only the inspection leaf private key, not the CA private
key. Non-loopback listeners require outer TLS. Mount private keys read-only
and restrict access to the gateway network.

The virtual key needs access to GET /v1/models for CONNECT authentication and
POST /v1/messages and /v1/messages/count_tokens. Normal key, model and policy checks still
run on every inference request. Key revocation does not forcibly disconnect
an existing opaque feature-service tunnel. Its next CONNECT reauthenticates.
connection_lifetime_seconds bounds that delay, even when the tunnel is active.

Launch Claude Code using standard environment configuration::

    unset ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN ANTHROPIC_API_KEY
    unset ANTHROPIC_CUSTOM_HEADERS
    export HTTPS_PROXY="https://litellm:${LITELLM_VIRTUAL_KEY}@gateway.example.com:4440"
    export NODE_EXTRA_CA_CERTS="/path/to/enterprise-trust-bundle.pem"
    claude

URL-encode a virtual key containing URL-reserved characters. Remove stale
provider flags and apiKeyHelper settings when switching to subscription
sign-in. Preserve any unrelated custom headers without the old gateway key.
Ensure neither NO_PROXY nor its lowercase variant bypasses api.anthropic.com.
Remove conflicting lowercase proxy variables, which some clients prefer.
Trust certificates for both TLS layers. Do not disable certificate verification.

Administrators can deploy these environment settings and the trust bundle through
their existing device-management system. Developers keep their native Claude
sign-in and start Claude Code normally. No additional local service is needed.
Gateway virtual-key rotation and Claude subscription renewal are independent.
Claude manages its subscription tokens. The organization distributes replacement
gateway keys and restarts clients that read proxy settings only at startup.
This listener does not implement an automatic gateway-key renewal service.

Limits and rollout
------------------

The listener supports HTTP/1.1 inside CONNECT and WebSocket upgrades. It
closes inspected connections after each HTTP request. Request bodies are
bounded at 32 MiB by default. The default is 256 concurrent connections per
worker with a 120-second idle timeout and a one-hour maximum connection lifetime.
Activity in either direction keeps a duplex tunnel alive until its lifetime limit.
shutdown_timeout_seconds defaults to 30 seconds. Shutdown first stops accepting
connections, then gives existing requests this interval to finish before cancelling
remaining work. Set the process or pod termination grace period above this interval
plus the five-second TLS close allowance. Rotate mounted certificates and keys by
restarting workers, with both old and new CA roots trusted during a CA rollover.
Linux and macOS workers share the
listening port. Use a single worker on Windows. Keep the gateway's existing
shared Redis coordination when running multiple workers.

tunnel_hosts replaces the default hostname allowlist. An entry beginning
with a dot permits subdomains. All destinations must resolve exclusively to
public addresses, and connections use the checked address. Additional MCP,
plugin, WebFetch or future feature hosts may need explicit allowlist entries
or direct connectivity outside this proxy. HTTP destinations are not handled
by this HTTPS-only listener.

service_paths replaces the default API-host service allowlist. A trailing slash
permits a path prefix. Other entries match an exact path, ignoring its query.
The defaults cover OAuth, CLI bootstrap and policy, feature evaluation, organization
model discovery, telemetry, MCP discovery, environments and sessions. Session and
environment routes are native control-plane services, not local Messages inference.
Unknown paths return 403. Enable debug logging to identify a denied path without
logging its query or payload, review its purpose, and update this configuration.
Do not approve inference endpoints as service paths. Known alternative inference
routes remain denied even when a service permission would match them.
New features may require a reviewed service-path or hostname entry. This is a
deliberate governance boundary rather than automatic compatibility with future APIs.

Local POST /v1/messages inference and token-count requests traverse LiteLLM's model pipeline.
Cloud sessions, scheduled tasks and cloud reviews execute on Anthropic's
infrastructure. Launching them through the listener does not route their
hosted model calls through this LiteLLM instance. Native plan entitlements,
organization policies and external integrations still apply.

Feature-service traffic is not interpreted as model inference and does not
receive inference guardrails or token accounting. The existing Messages
pipeline still determines supported inference fields and headers. New
inference endpoints, protocols or client-side identity checks require review.
Neither opaque tunnels nor a subscription login can guarantee compatibility
with unannounced Anthropic features.

The listener assigns the primary inference a fresh request ID and ignores a client
supplied ID. The credential policy follows this ID across deployment attempts and
also covers any auxiliary call that forwards the subscription credential. Model-based
guardrails can use independent call IDs and their own provider credentials without
inheriting the subscription identity. The SDK assigns independent calls their own IDs.

Validate each client release before rollout and monitor native feature
failures as well as gateway inference failures. Full repository CI, security
review, certificate rotation and sustained load testing are required before
production adoption. Disabling this configuration removes the listener.
Clients must then remove HTTPS_PROXY or return to their previous gateway
configuration.

Reference documentation
-----------------------

https://code.claude.com/docs/en/network-config

https://code.claude.com/docs/en/feature-availability

https://docs.litellm.ai/docs/tutorials/claude_code_max_subscription
