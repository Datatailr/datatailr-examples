import pytest

from llm_service.deploy_config import admin_app_name, deploy_targets, resources, service_env, service_url


def test_service_env_defaults_hold_only_the_two_required_variables():
    assert service_env({}) == {
        "LLM_DEFAULT_MODEL": "Qwen/Qwen2.5-1.5B-Instruct",
        "LLM_BLOB_ROOT": "llm-service",
    }


def test_service_env_passes_overrides_and_optional_variables():
    env = {
        "LLM_DEFAULT_MODEL": "org/model",
        "LLM_BLOB_ROOT": "/my-root/",
        "LLM_ADMIN_GROUP": "llm_admins",
        "LLM_ACCESS_GROUP": "llm_users",
        "LLM_HF_TOKEN_KEY": "hf_token",
        "LLM_VLLM_ARGS": "--max-model-len 4096",
        "LLM_PROMPT_RETENTION_DAYS": "30",
    }
    assert service_env(env) == {
        "LLM_DEFAULT_MODEL": "org/model",
        "LLM_BLOB_ROOT": "my-root",
        "LLM_ADMIN_GROUP": "llm_admins",
        "LLM_ACCESS_GROUP": "llm_users",
        "LLM_HF_TOKEN_KEY": "hf_token",
        "LLM_VLLM_ARGS": "--max-model-len 4096",
        "LLM_PROMPT_RETENTION_DAYS": "30",
    }


def test_service_env_leaves_out_empty_optional_variables():
    env = {"LLM_ADMIN_GROUP": "", "LLM_ACCESS_GROUP": " ", "LLM_HF_TOKEN_KEY": "  ", "LLM_VLLM_ARGS": "", "LLM_PROMPT_RETENTION_DAYS": " "}
    assert set(service_env(env)) == {"LLM_DEFAULT_MODEL", "LLM_BLOB_ROOT"}


def test_service_env_ignores_variables_the_container_does_not_need():
    env = {"LLM_VRAM": "40g", "OTHER": "x"}
    assert set(service_env(env)) == {"LLM_DEFAULT_MODEL", "LLM_BLOB_ROOT"}


def test_resources_defaults():
    assert resources({}) == {"memory": "24g", "cpu": 4.0, "vram": "16g"}


def test_resources_overrides():
    env = {"LLM_MEMORY": "64g", "LLM_CPU": "8", "LLM_VRAM": "40g"}
    assert resources(env) == {"memory": "64g", "cpu": 8.0, "vram": "40g"}


def test_resources_empty_values_use_the_defaults():
    env = {"LLM_MEMORY": "", "LLM_CPU": " ", "LLM_VRAM": ""}
    assert resources(env) == {"memory": "24g", "cpu": 4.0, "vram": "16g"}


def test_service_url_turns_the_name_into_a_host_name():
    assert service_url("LLM Service") == "http://llm-service"
    assert service_url("My  LLM_Service!") == "http://my-llm-service"
    assert service_url("--Team 1--") == "http://team-1"


def test_service_url_applies_nfkc_before_the_lower_case_step():
    # Full-width letters and digits become ASCII, as on the platform.
    assert service_url("\uff2c\uff2c\uff2d \uff33ervice \uff11") == "http://llm-service-1"


def test_admin_app_name_default_and_override():
    assert admin_app_name({}) == "LLM Service Admin"
    assert admin_app_name({"LLM_ADMIN_APP_NAME": "  "}) == "LLM Service Admin"
    assert admin_app_name({"LLM_ADMIN_APP_NAME": " My Admin "}) == "My Admin"


def test_deploy_targets_accepts_nothing_or_one_target():
    assert deploy_targets([]) == ("service", "app")
    assert deploy_targets(["service"]) == ("service",)
    assert deploy_targets(["app"]) == ("app",)


@pytest.mark.parametrize("argv", [["both"], ["service", "app"], [""], ["App"]])
def test_deploy_targets_rejects_anything_else(argv):
    with pytest.raises(ValueError, match=r"usage: python deploy.py \[service\|app\]"):
        deploy_targets(argv)
