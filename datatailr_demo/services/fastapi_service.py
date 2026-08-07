import random

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

app = FastAPI(
    title="Data Service",
    description="Simple Datatailr demo service with auto-generated OpenAPI docs.",
    version="1.0.0",
)


class RandomNumberResponse(BaseModel):
    random_number: int = Field(..., description="Random integer in the requested range")


class GreetingResponse(BaseModel):
    greeting: str = Field(..., description="Greeting message")


@app.get(
    "/random",
    response_model=RandomNumberResponse,
    summary="Generate a random number",
    tags=["demo"],
)
def random_number(
    min_val: int = Query(0, alias="min", description="Inclusive lower bound"),
    max_val: int = Query(100, alias="max", description="Inclusive upper bound"),
) -> RandomNumberResponse:
    value = random.randint(min_val, max_val)
    return RandomNumberResponse(random_number=value)


@app.get("/", response_class=PlainTextResponse, summary="Service index", tags=["system"])
def index() -> str:
    return "Data Service running on datatailr!\n"


@app.get("/health", response_class=PlainTextResponse, summary="Health check", tags=["system"])
def health_check() -> str:
    return "OK\n"


@app.get(
    "/greet",
    response_model=GreetingResponse,
    summary="Greet by name",
    tags=["demo"],
)
def greet(
    name: str = Query("World", description="Name to greet"),
) -> GreetingResponse:
    return GreetingResponse(greeting=f"Hello, {name} from Data Service!")


def main(port):
    uvicorn.run(app, host="0.0.0.0", port=int(port), log_level="info")


if __name__ == "__main__":
    main(1024)
