import pytest

from llm_service.gateway.limiter import WINDOW_SECONDS, LimitExceeded, Limiter

NONE = {"requests_per_minute": None, "tokens_per_day": None, "parallel_requests": None}


def limits(**kw):
    return {**NONE, **kw}


class Env:
    def __init__(self):
        self.now = 1000.0
        self.tokens = {}
        self.midnight = 3600.0
        self.limiter = Limiter(
            tokens_today=lambda user: self.tokens.get(user, 0),
            seconds_to_midnight=lambda: self.midnight,
            clock=lambda: self.now,
        )


@pytest.fixture
def env():
    return Env()


def test_window_is_sixty_seconds():
    assert WINDOW_SECONDS == 60.0


def test_requests_per_minute_at_limit_passes_then_refuses(env):
    lim = limits(requests_per_minute=2)
    env.limiter.acquire("alice", lim)
    env.limiter.acquire("alice", lim)
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", lim)
    err = info.value
    assert err.code == "requests_per_minute"
    assert err.limit == 2
    assert err.message == "You reached the limit of 2 requests in one minute."
    assert err.retry_after == 60


def test_window_frees_a_slot_after_sixty_seconds(env):
    lim = limits(requests_per_minute=1)
    env.limiter.acquire("alice", lim)
    env.now += 59.9
    with pytest.raises(LimitExceeded):
        env.limiter.acquire("alice", lim)
    env.now += 0.1
    env.limiter.acquire("alice", lim)


def test_retry_after_at_several_moments(env):
    lim = limits(requests_per_minute=1)
    env.limiter.acquire("alice", lim)
    expected = [(0.0, 60), (0.5, 60), (10.0, 50), (59.0, 1), (59.5, 1), (59.99, 1)]
    for delta, seconds in expected:
        env.now = 1000.0 + delta
        with pytest.raises(LimitExceeded) as info:
            env.limiter.acquire("alice", lim)
        assert info.value.retry_after == seconds, delta


def test_retry_after_uses_the_oldest_entry(env):
    lim = limits(requests_per_minute=2)
    env.limiter.acquire("alice", lim)
    env.now += 30
    env.limiter.acquire("alice", lim)
    env.now += 10
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", lim)
    assert info.value.retry_after == 20


def test_refusal_adds_nothing_to_window_or_counter(env):
    lim = limits(requests_per_minute=1)
    env.limiter.acquire("alice", lim)
    for _ in range(3):
        with pytest.raises(LimitExceeded):
            env.limiter.acquire("alice", lim)
    assert env.limiter.use("alice") == {
        "requests_last_minute": 1,
        "tokens_today": 0,
        "parallel_requests": 1,
    }
    env.now += 60
    env.limiter.acquire("alice", lim)
    with pytest.raises(LimitExceeded):
        env.limiter.acquire("alice", lim)


def test_parallel_requests_at_limit_passes_then_refuses(env):
    lim = limits(parallel_requests=2)
    env.limiter.acquire("alice", lim)
    env.limiter.acquire("alice", lim)
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", lim)
    err = info.value
    assert err.code == "parallel_requests"
    assert err.limit == 2
    assert err.message == "You reached the limit of 2 parallel requests."
    assert err.retry_after == 1
    assert env.limiter.use("alice")["parallel_requests"] == 2
    assert env.limiter.use("alice")["requests_last_minute"] == 2


def test_release_frees_a_parallel_slot(env):
    lim = limits(parallel_requests=1)
    env.limiter.acquire("alice", lim)
    with pytest.raises(LimitExceeded):
        env.limiter.acquire("alice", lim)
    env.limiter.release("alice")
    env.limiter.acquire("alice", lim)


def test_release_without_acquire_keeps_zero(env):
    env.limiter.release("alice")
    env.limiter.release("alice")
    assert env.limiter.use("alice")["parallel_requests"] == 0
    assert env.limiter.active_users() == []
    env.limiter.acquire("alice", limits(parallel_requests=1))
    env.limiter.release("alice")
    env.limiter.release("alice")
    assert env.limiter.use("alice")["parallel_requests"] == 0
    env.limiter.acquire("alice", limits(parallel_requests=1))
    with pytest.raises(LimitExceeded):
        env.limiter.acquire("alice", limits(parallel_requests=1))


def test_tokens_per_day_refuses_at_the_budget(env):
    lim = limits(tokens_per_day=100)
    env.tokens["bob"] = 99
    env.limiter.acquire("bob", lim)
    env.tokens["bob"] = 100
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("bob", lim)
    err = info.value
    assert err.code == "tokens_per_day"
    assert err.limit == 100
    assert err.message == "You reached the limit of 100 tokens in one day."
    assert err.retry_after == 3600
    assert env.limiter.use("bob")["requests_last_minute"] == 1
    assert env.limiter.use("bob")["parallel_requests"] == 1


def test_tokens_retry_after_rounds_up_and_is_at_least_one(env):
    lim = limits(tokens_per_day=1)
    env.tokens["bob"] = 1
    env.midnight = 100.2
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("bob", lim)
    assert info.value.retry_after == 101
    env.midnight = 0.0
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("bob", lim)
    assert info.value.retry_after == 1
    env.midnight = 0.3
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("bob", lim)
    assert info.value.retry_after == 1


def test_null_limits_never_refuse(env):
    env.tokens["carol"] = 10**9
    for _ in range(500):
        env.limiter.acquire("carol", NONE)
    assert env.limiter.use("carol")["parallel_requests"] == 500
    assert env.limiter.use("carol")["requests_last_minute"] == 500


def test_limits_are_per_user(env):
    lim = limits(parallel_requests=1, requests_per_minute=1)
    env.limiter.acquire("alice", lim)
    env.limiter.acquire("bob", lim)
    with pytest.raises(LimitExceeded):
        env.limiter.acquire("alice", lim)


def test_order_parallel_before_requests_before_tokens(env):
    env.tokens["alice"] = 10
    all_three = limits(requests_per_minute=1, tokens_per_day=10, parallel_requests=1)
    env.limiter.acquire("alice", limits(requests_per_minute=5))
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", all_three)
    assert info.value.code == "parallel_requests"
    env.limiter.release("alice")
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", all_three)
    assert info.value.code == "requests_per_minute"
    env.now += 60
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", all_three)
    assert info.value.code == "tokens_per_day"


def test_use_and_active_users_through_a_request(env):
    assert env.limiter.use("alice") == {
        "requests_last_minute": 0,
        "tokens_today": 0,
        "parallel_requests": 0,
    }
    assert env.limiter.active_users() == []
    env.tokens["alice"] = 42
    env.limiter.acquire("bob", NONE)
    env.limiter.acquire("alice", NONE)
    assert env.limiter.use("alice") == {
        "requests_last_minute": 1,
        "tokens_today": 42,
        "parallel_requests": 1,
    }
    assert env.limiter.active_users() == ["alice", "bob"]
    env.limiter.release("alice")
    env.limiter.release("bob")
    assert env.limiter.use("alice")["parallel_requests"] == 0
    assert env.limiter.use("alice")["requests_last_minute"] == 1
    assert env.limiter.active_users() == ["alice", "bob"]
    env.now += 59.9
    assert env.limiter.active_users() == ["alice", "bob"]
    env.now += 0.1
    assert env.limiter.use("alice")["requests_last_minute"] == 0
    assert env.limiter.active_users() == []


def test_user_in_flight_stays_active_after_the_window_empties(env):
    env.limiter.acquire("alice", NONE)
    env.now += 120
    assert env.limiter.active_users() == ["alice"]
    assert env.limiter.use("alice")["requests_last_minute"] == 0
    env.limiter.release("alice")
    assert env.limiter.active_users() == []


def test_user_leaves_memory_after_sixty_seconds(env):
    env.limiter.acquire("alice", NONE)
    env.limiter.release("alice")
    env.now += 61
    env.limiter.acquire("bob", NONE)
    assert "alice" not in env.limiter._users
    assert "bob" in env.limiter._users


def test_use_does_not_create_memory_for_unknown_users(env):
    env.limiter.use("alice")
    env.limiter.release("alice")
    assert env.limiter._users == {}


@pytest.mark.parametrize("name", ["requests_per_minute", "tokens_per_day", "parallel_requests"])
@pytest.mark.parametrize("value", [0, -1])
@pytest.mark.parametrize("busy", [False, True])
def test_limit_below_one_refuses_each_request(env, name, value, busy):
    if busy:
        env.limiter.acquire("alice", NONE)
    before = env.limiter.use("alice")
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", limits(**{name: value}))
    assert info.value.code == name
    assert info.value.limit == value
    assert info.value.retry_after >= 1
    assert env.limiter.use("alice") == before


def test_zero_requests_per_minute_retry_after_for_empty_window(env):
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", limits(requests_per_minute=0))
    assert info.value.retry_after == 60
    env.limiter.acquire("alice", NONE)
    env.limiter.release("alice")
    env.now += 10
    with pytest.raises(LimitExceeded) as info:
        env.limiter.acquire("alice", limits(requests_per_minute=0))
    assert info.value.retry_after == 50


def test_acquire_sweeps_all_users_at_most_once_a_second(env):
    env.limiter.acquire("alice", NONE)
    env.limiter.release("alice")
    env.now += 59.5
    env.limiter.acquire("carol", NONE)
    assert "alice" in env.limiter._users
    env.now += 0.7
    env.limiter.acquire("bob", NONE)
    assert "alice" in env.limiter._users
    env.now += 0.3
    env.limiter.acquire("dave", NONE)
    assert "alice" not in env.limiter._users


def test_acquire_trims_the_calling_user_between_sweeps(env):
    env.limiter.acquire("alice", NONE)
    env.limiter.release("alice")
    env.now += 59.9
    env.limiter.acquire("bob", NONE)
    env.now += 0.2
    env.limiter.acquire("alice", limits(requests_per_minute=1))
