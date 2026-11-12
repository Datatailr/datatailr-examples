import pytest

from llm_service.gateway.config import Config


def test_defaults():
    c = Config.from_env({})
    assert c.default_model == "Qwen/Qwen2.5-1.5B-Instruct"
    assert c.blob_root == "llm-service"
    assert c.admin_group is None and c.hf_token_key is None and c.access_group is None
    assert c.vllm_args == [] and c.engine_port == 8001
    assert c.owner == "" and str(c.local_dir) == "/tmp/llm-service"
    assert c.flush_interval == 5.0 and c.flush_size == 100


def test_overrides():
    c = Config.from_env(
        {
            "LLM_BLOB_ROOT": "/x/",
            "LLM_VLLM_ARGS": "--max-model-len 4096",
            "LLM_ADMIN_GROUP": "llm-admins",
            "DATATAILR_USER": "alice",
        }
    )
    assert c.blob_root == "x"  # slashes stripped
    assert c.vllm_args == ["--max-model-len", "4096"]
    assert c.admin_group == "llm-admins" and c.owner == "alice"


def test_more_overrides():
    c = Config.from_env(
        {
            "LLM_DEFAULT_MODEL": "org/model",
            "LLM_HF_TOKEN_KEY": "hf",
            "LLM_LOCAL_DIR": "/data/w",
            "LLM_ENGINE_PORT": "9000",
        }
    )
    assert c.default_model == "org/model" and c.hf_token_key == "hf"
    assert str(c.local_dir) == "/data/w" and c.engine_port == 9000


def test_prompt_retention_days():
    assert Config.from_env({}).prompt_retention_days is None
    assert Config.from_env({"LLM_PROMPT_RETENTION_DAYS": ""}).prompt_retention_days is None
    assert Config.from_env({"LLM_PROMPT_RETENTION_DAYS": "  "}).prompt_retention_days is None
    assert Config.from_env({"LLM_PROMPT_RETENTION_DAYS": "7"}).prompt_retention_days == 7
    assert Config.from_env({"LLM_PROMPT_RETENTION_DAYS": " 30 "}).prompt_retention_days == 30


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "x", "7d"])
def test_prompt_retention_days_rejects_anything_else(value):
    with pytest.raises(ValueError, match="LLM_PROMPT_RETENTION_DAYS"):
        Config.from_env({"LLM_PROMPT_RETENTION_DAYS": value})


def test_prompt_retention_days_upper_bound():
    assert Config.from_env({"LLM_PROMPT_RETENTION_DAYS": "36500"}).prompt_retention_days == 36500
    for value in ("36501", "9" * 5000):
        with pytest.raises(ValueError, match="LLM_PROMPT_RETENTION_DAYS"):
            Config.from_env({"LLM_PROMPT_RETENTION_DAYS": value})


def test_access_group():
    assert Config.from_env({"LLM_ACCESS_GROUP": "team"}).access_group == "team"
    assert Config.from_env({"LLM_ACCESS_GROUP": ""}).access_group is None


def test_job_name_and_environment():
    c = Config.from_env({"DATATAILR_JOB_NAME": "llm-service", "DATATAILR_JOB_ENVIRONMENT": "dev"})
    assert c.job_name == "llm-service" and c.environment == "dev"
