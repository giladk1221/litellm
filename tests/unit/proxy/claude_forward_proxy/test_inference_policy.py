from collections.abc import Mapping
from typing import Final

import pytest

from litellm._internal_context import native_anthropic_subscription, native_anthropic_subscription_token
from litellm.exceptions import BadRequestError
from litellm.llms.anthropic.common_utils import AnthropicModelInfo
from litellm.proxy.claude_forward_proxy.inference_policy import NativeInferencePolicy
from litellm.proxy.claude_forward_proxy.settings import ClaudeForwardProxySettings
from litellm.proxy.rust_forward.mode import validate_claude_listener_runtime
from litellm.types.utils import CallTypes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "target",
    [
        {"model": "openai/test"},
        {"model": "bedrock/anthropic.test"},
        {"model": "anthropic/test", "api_base": "https://unapproved.example"},
        {"model": "anthropic/test", "api_base": "http://api.anthropic.com"},
        {"model": "anthropic/test", "api_base": "https://api.anthropic.com.evil.example"},
        {"model": "anthropic/test", "api_key": "server-owned-key"},
        {"model": "anthropic/test", "litellm_credential_name": "another-account"},
    ],
)
async def test_native_requests_reject_non_subscription_targets(target: Mapping[str, object]) -> None:
    policy: Final = NativeInferencePolicy()
    with native_anthropic_subscription("sk-ant-oat01-test"):
        with pytest.raises(BadRequestError, match="Native Claude inference"):
            await policy.async_pre_call_deployment_hook(dict(target), CallTypes.anthropic_messages)


@pytest.mark.asyncio
async def test_native_policy_disables_response_cache_without_changing_regular_requests() -> None:
    policy: Final = NativeInferencePolicy()
    request: Final[dict[str, object]] = {"model": "anthropic/test", "cache": {"no-cache": False}}
    with native_anthropic_subscription("sk-ant-oat01-test"):
        native: Final = await policy.async_pre_call_deployment_hook(request, CallTypes.anthropic_messages)
    assert native is not None
    assert native["cache"] == {"no-cache": True, "no-store": True}
    assert request["cache"] == {"no-cache": False}
    assert native_anthropic_subscription_token() is None
    assert await policy.async_pre_call_deployment_hook(request, CallTypes.anthropic_messages) is None
    assert await policy.async_pre_call_deployment_hook({"model": "openai/test"}, CallTypes.acompletion) is None


@pytest.mark.parametrize(
    "base", ["https://unapproved.example", "http://api.anthropic.com", "https://api.anthropic.com:444"]
)
def test_native_authentication_never_builds_credentials_for_another_origin(base: str) -> None:
    with native_anthropic_subscription("sk-ant-oat01-test"):
        with pytest.raises(BadRequestError, match="Anthropic API origin"):
            AnthropicModelInfo.get_auth_header(api_base=base)


def test_native_token_count_auth_uses_the_client_account_and_restores_outer_context() -> None:
    with native_anthropic_subscription("sk-ant-oat01-outer"):
        with native_anthropic_subscription("sk-ant-oat01-inner"):
            headers: Final = AnthropicModelInfo.get_auth_header(api_key="server-owned-key")
            assert headers is not None
            assert headers["authorization"] == "Bearer sk-ant-oat01-inner"
            assert "x-api-key" not in headers
        assert native_anthropic_subscription_token() == "sk-ant-oat01-outer"
    assert native_anthropic_subscription_token() is None


@pytest.mark.parametrize("path", ["/", "/api/", "/v1/", "/api/../v1/", "/api/%2e/", "/api/service?token=x"])
def test_enabled_configuration_rejects_ambiguous_or_overbroad_service_permissions(path: str) -> None:
    with pytest.raises(ValueError, match="service_paths"):
        ClaudeForwardProxySettings(enabled=True, certificate="cert.pem", private_key="key.pem", service_paths=(path,))


@pytest.mark.parametrize("host", [".anthropic.com", "api.anthropic.com", ".", "claude.ai.", "claude.ai/evil"])
def test_enabled_configuration_rejects_unsafe_tunnel_destinations(host: str) -> None:
    with pytest.raises(ValueError, match="tunnel_hosts"):
        ClaudeForwardProxySettings(enabled=True, certificate="cert.pem", private_key="key.pem", tunnel_hosts=(host,))


def test_supervised_runtime_is_refused_without_affecting_regular_python_startup() -> None:
    validate_claude_listener_runtime({})
    with pytest.raises(ValueError, match="LITELLM_RUST_FIRST"):
        validate_claude_listener_runtime({"LITELLM_RUST_FIRST": "true"})
