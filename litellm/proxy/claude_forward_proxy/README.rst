Claude subscription forward proxy
=================================

This opt-in listener lets Claude Code keep its native subscription sign-in
and API hostname while LiteLLM processes local Messages API requests. It
runs inside the Python proxy workers. There is no client bridge process.
This is an experimental integration, not a certification of every Claude
feature or future client release.

Transport and authentication
----------------------------

Claude Code connects with HTTPS CONNECT using a LiteLLM virtual key in
proxy authentication. The listener terminates the inner TLS connection only
for api.anthropic.com. POST /v1/messages enters the existing LiteLLM ASGI
application, including its authentication, routing, policies and accounting.
The user's original subscription OAuth header remains the provider credential.
Configure direct anthropic/* deployments and scope virtual keys accordingly.
Keep fallback deployments on direct Anthropic too.

Other paths on that host retain their native request bodies and credentials,
including feature-service WebSocket upgrades. Other allowed HTTPS hosts use
opaque CONNECT tunnels. LiteLLM credentials are not sent to upstream services.
Claude Code remains responsible for storing and refreshing its own sign-in.

Configuration
-------------

Python 3.11 or later is required when the listener is enabled. Leave
claude_forward_proxy absent to retain the existing startup behavior.
An authenticated gateway with a configured LiteLLM master key is required.

Add this configuration to the existing proxy configuration::

    model_list:
      - model_name: anthropic/*
        litellm_params:
          model: anthropic/*
    general_settings:
      claude_forward_proxy:
        host: 0.0.0.0
        port: 4440
        certificate: /etc/litellm/tls/anthropic-inspection.pem
        private_key: /etc/litellm/tls/anthropic-inspection.key
        listener_certificate: /etc/litellm/tls/proxy-server.pem
        listener_private_key: /etc/litellm/tls/proxy-server.key

The inspection certificate must cover api.anthropic.com and chain to a CA
trusted by the Claude Code process. The listener certificate must cover the
gateway's actual hostname. Use an enterprise-managed certificate lifecycle.
The listener needs only the inspection leaf private key, not the CA private
key. Non-loopback listeners require outer TLS. Mount private keys read-only
and restrict access to the gateway network.

The virtual key needs access to GET /v1/models for CONNECT authentication and
POST /v1/messages for inference. Normal key, model and policy checks still
run on every inference request. Key revocation does not forcibly disconnect
an existing opaque feature-service tunnel. Its next CONNECT reauthenticates.

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
Trust certificates for both TLS layers. Do not disable certificate verification.

Limits and rollout
------------------

The listener supports HTTP/1.1 inside CONNECT and WebSocket upgrades. It
closes inspected connections after each HTTP request. Request bodies are
bounded at 32 MiB by default. The default is 256 concurrent connections per
worker with a 120-second idle timeout. Linux and macOS workers share the
listening port. Use a single worker on Windows. Keep the gateway's existing
shared Redis coordination when running multiple workers.

tunnel_hosts replaces the default hostname allowlist. An entry beginning
with a dot permits subdomains. All destinations must resolve exclusively to
public addresses, and connections use the checked address. Additional MCP,
plugin, WebFetch or future feature hosts may need explicit allowlist entries
or direct connectivity outside this proxy. HTTP destinations are not handled
by this HTTPS-only listener.

Only local POST /v1/messages inference traverses LiteLLM's model pipeline.
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
