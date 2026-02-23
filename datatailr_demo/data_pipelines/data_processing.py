# *************************************************************************
#
#  Copyright (c) 2026 - Datatailr Inc.
#  All Rights Reserved.
#
#  This file is part of Datatailr and subject to the terms and conditions
#  defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
#  of this file, in parts or full, via any medium is strictly prohibited.
# *************************************************************************
from typing import Any

from datatailr.logging import DatatailrLogger

# --8<-- [start:import_example]
from datatailr import task
# --8<-- [end:import_example]

logger = DatatailrLogger(__name__).get_logger()


# --8<-- [start:task_example]
@task(memory="123m", cpu=0.5)
def func_no_args() -> str:
    return "no_args"


# --8<-- [end:task_example]


@task()
def get_number() -> int:
    return 42


@task()
def get_number_from_service(min_val: int, max_val: int) -> int:
    import requests

    data_service_url = "http://simple-service-<>USERNAME<>"
    try:
        r = requests.get(
            f"{data_service_url}/random",
            params={"min": int(min_val), "max": int(max_val)},
            timeout=2,
        )
        if r.status_code == 200:
            rand_num = r.json().get("random_number", None)
            logger.info(f"Received random number {rand_num} from data service")
            return rand_num
        else:
            logger.error("Data service error on /random.")
            return -1
    except Exception as e:
        logger.error(f"Error getting random number from data service: {e}")
        return 42


@task()
def add(a, b) -> Any:
    return a + b


@task()
def double(n: int) -> int:
    return n * 2


def receive_one_number(n: int) -> int:
    return n + 1


@task()
def receive_one_number_job(n: int) -> int:
    return receive_one_number(n)


def receive_two_numbers(a: int, b: int) -> int:
    return a + b


@task()
def receive_two_numbers_job(a: int, b: int) -> int:
    return receive_two_numbers(a, b)


def receive_varargs(*args: int) -> int:
    return sum(args)


@task()
def receive_varargs_job(*args) -> int:
    return receive_varargs(*args)


@task()
def func_with_args(a, b) -> str:
    return f"args: {a}, {b}"


@task()
def func_with_kwargs(a: int, b: float, c: int = 0) -> str:
    return f"args: {a}, {b}, kwargs: {c}"


@task()
def func_with_varargs(*args) -> str:
    return f"args: {args}"


@task()
def function_with_system_arguments(
    rundate, scheduled_time, started_at, batch_name, job_name
):
    logger.info(
        f"Running function_with_system_arguments from test_submodule, {__name__}"
    )
    logger.info(
        f"Arguments: rundate={rundate}, scheduled_time={scheduled_time}, "
        f"started_at={started_at}, batch_name={batch_name}, job_name={job_name}"
    )
    return f"args: ({rundate}, {scheduled_time}, {started_at}, {batch_name}, {job_name}), kwargs: {{}}"


@task()
def function_with_partial_system_args(rundate, batch_name, job_name):
    logger.info(
        f"Running function_with_partial_system_args from test_submodule, {__name__}"
    )
    logger.info(
        f"Arguments: rundate={rundate}, batch_name={batch_name}, job_name={job_name}"
    )
    return f"args: ({rundate}, {batch_name}, {job_name}), kwargs: {{}}"


@task()
def function_with_args_and_varargs(rundate, *args):
    logger.info(
        f"Running function_with_args_and_varargs from test_submodule, {__name__}"
    )
    logger.info(f"Arguments: rundate={rundate}, args={args}")
    return f"args: ({rundate}, {args}), kwargs: {{}}"


@task()
def get_data() -> dict:
    return {"a": 1, "b": 2, "c": 3}


@task()
def process_data(data: dict) -> dict:
    data["sum"] = sum(data.values())
    return data
