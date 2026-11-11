from llm_service.gateway.config import Config


def test_defaults():
    c = Config.from_env({})
    assert c.default_model == "Qwen/Qwen2.5-1.5B-Instruct"
    assert c.blob_root == "llm-service"
    assert c.admin_group is None and c.hf_token_key is None
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
