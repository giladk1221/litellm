from collections.abc import Mapping
from typing import Final

from litellm._internal_context import native_anthropic_subscription_token
from litellm.exceptions import BadRequestError
from litellm.integrations.custom_logger import CustomLogger
from litellm.litellm_core_utils.get_llm_provider_logic import get_llm_provider
from litellm.llms.anthropic.common_utils import AnthropicModelInfo
from litellm.types.utils import CallTypes


def refused_target(request: Mapping[str, object]) -> str | None:
    model: Final = request.get("model")
    provider: Final = request.get("custom_llm_provider")
    base: Final = request.get("api_base")
    if not isinstance(model, str) or (provider is not None and not isinstance(provider, str)):
        return "Native Claude inference requires a resolved Anthropic model"
    if base is not None and not isinstance(base, str):
        return "Native Claude inference requires the Anthropic API origin"
    _, resolved_provider, _, resolved_base = get_llm_provider(model, custom_llm_provider=provider, api_base=base)
    if resolved_provider != "anthropic":
        return "Native Claude inference cannot use another provider, including a fallback"
    if not AnthropicModelInfo.is_subscription_origin(
        AnthropicModelInfo.resolve_anthropic_api_base(resolved_base or base)
    ):
        return "Native Claude inference requires the Anthropic API origin"
    if any(request.get(name) for name in ("api_key", "litellm_credential_name", "oauth_client_credentials")):
        return "Native Claude inference requires the caller's subscription credential"
    return None


def validate_native_target(request: Mapping[str, object]) -> None:
    if native_anthropic_subscription_token() is None:
        return
    refusal: Final = refused_target(request)
    if refusal is not None:
        model: Final = request.get("model")
        raise BadRequestError(
            message=refusal,
            model=model if isinstance(model, str) else "",
            llm_provider="anthropic",
        )


class NativeInferencePolicy(CustomLogger):
    async def async_pre_call_deployment_hook(
        self, kwargs: dict[str, object], call_type: CallTypes | None
    ) -> dict[str, object] | None:
        if native_anthropic_subscription_token() is None:
            return None
        validate_native_target(kwargs)
        return {**kwargs, "cache": {"no-cache": True, "no-store": True}}
