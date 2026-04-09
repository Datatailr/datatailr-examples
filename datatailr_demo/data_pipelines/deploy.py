from datatailr import workflow

from datatailr_demo.data_pipelines.data_processing import (
    get_data,
    process_data,
    get_number,
    use_rundate,
    add,
    get_number_from_service,
)

@workflow(name="Simple Data Pipeline", python_requirements=["requests"])
def simple_data_pipeline():
    data = get_data()
    process_data(data)

    use_rundate()
    a = get_number().alias("a")
    b = get_number().alias("b")
    add(a, b).alias("Add a and b")
    add(a, 18).alias("Add a and 18")
    rand_low = get_number_from_service(0, 10).alias("Random 0-10")
    rand_high = get_number_from_service(90, 100).alias("Random 90-100")
    random = get_number_from_service(rand_low, rand_high).alias(
        "Random between previous two"
    )

if __name__ == "__main__":
    # run locally with the rundate set in the environment variables
    import os
    os.environ["DATATAILR_BATCH_ARG_RUNDATE"] = "2026-04-09"
    simple_data_pipeline(local_run=True)