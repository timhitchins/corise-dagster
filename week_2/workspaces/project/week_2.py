from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},  # use with context
    tags={"kind": "s3"},
    description="Get stock data from s3 bucket.",
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    # use the s3 resource key to call s3 client method get_data
    s3_lines = context.resources.s3.get_data(s3_key)
    stocks = list(map(Stock.from_list, s3_lines))
    context.log.info("Fetched s3 stocks data")
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Filter and return highest stock (mock aggregation).",
)
def process_data(context, stocks):
    # use max with lambda for highest stock
    high_stock = max(stocks, key=lambda x: x.high)
    aggregation = Aggregation(date=high_stock.date, high=high_stock.high)
    context.log.info("Aggregation complete")
    return aggregation


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
    description="Writes to redis cache",
)
def put_redis_data(context: OpExecutionContext, aggregation):
    name = str(aggregation.date)
    value = str(aggregation.high)
    # use the redis resource key to call redis client method put_data
    context.resources.redis.put_data(name, value)
    context.log.info("Aggregation written to redis cache")


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Aggregation written to s3.",
)
def put_s3_data(context, aggregation):
    key_name = str(aggregation.date)
    # use the s3 resource key to call the client method put_data
    context.resources.s3.put_data(key_name, aggregation)
    context.log.info("Aggregation written to S3")


@graph
def machine_learning_graph():
    s3_data = get_s3_data()
    aggregation = process_data(s3_data)
    put_redis_data(aggregation)
    put_s3_data(aggregation)


# set resource and op confgs to pass to jobs
local = {
    "resources": {
        "redis": {"config": REDIS},
    },
    "ops": {
        "get_s3_data": {"config": {"s3_key": S3_FILE}},
    },
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

# Call job on graph
machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    # use mocks
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    # use production resources
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)

# known bug:
# machine_learning_job_docker won't run in
# dagit as a job because localstack is not
# the dirty fix is to start it manually:
# docker exec -it <container id> bash
