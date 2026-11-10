from llm_service.deploy_config import resources, service_env


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
        "LLM_HF_TOKEN_KEY": "hf_token",
        "LLM_VLLM_ARGS": "--max-model-len 4096",
    }
    assert service_env(env) == {
        "LLM_DEFAULT_MODEL": "org/model",
        "LLM_BLOB_ROOT": "my-root",
        "LLM_ADMIN_GROUP": "llm_admins",
        "LLM_HF_TOKEN_KEY": "hf_token",
        "LLM_VLLM_ARGS": "--max-model-len 4096",
    }


def test_service_env_leaves_out_empty_optional_variables():
    env = {"LLM_ADMIN_GROUP": "", "LLM_HF_TOKEN_KEY": "  ", "LLM_VLLM_ARGS": ""}
    assert set(service_env(env)) == {"LLM_DEFAULT_MODEL", "LLM_BLOB_ROOT"}


def test_service_env_ignores_variables_the_container_does_not_need():
    env = {"LLM_VRAM": "40g", "LLM_ACCESS_GROUP": "team", "OTHER": "x"}
    assert set(service_env(env)) == {"LLM_DEFAULT_MODEL", "LLM_BLOB_ROOT"}


def test_resources_defaults():
    assert resources({}) == {"memory": "24g", "cpu": 4.0, "vram": "16g"}


def test_resources_overrides():
    env = {"LLM_MEMORY": "64g", "LLM_CPU": "8", "LLM_VRAM": "40g"}
    assert resources(env) == {"memory": "64g", "cpu": 8.0, "vram": "40g"}


def test_resources_empty_values_use_the_defaults():
    env = {"LLM_MEMORY": "", "LLM_CPU": " ", "LLM_VRAM": ""}
    assert resources(env) == {"memory": "24g", "cpu": 4.0, "vram": "16g"}
