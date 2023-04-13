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

# replace that functionality with our S3 resource and use the S3 client method get_data
# to read the contents a file from remote storage
# (in this case our localstack version  of S3 within Docker).
@op(
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get stock data from s3 bucket.",
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    # use the s3 resource key to call client method
    stocks = list(context.resources.s3.get_data(s3_key))  # ? why does this not return Stock?
    context.log.info("Fetched s3 stocks data")
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Filter and return highest stock (mock aggregation).",
)
def process_data(context, stocks):
    high_stock = max(stocks, key=lambda x: x.high)
    aggregation = Aggregation(date=high_stock.date, high=high_stock.high)
    return aggregation


@op
def put_redis_data():
    pass


@op(
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    tags={"kind": "s3"},
    description="Upload aggregation to s3.",
)
def put_s3_data(context: OpExecutionContext, aggregation):
    s3_key = context.op_config["s3_key"]
    context.resources.s3.put_data(s3_key, aggregation)


@graph
def machine_learning_graph():
    s3_data = get_s3_data()
    aggregation = process_data(s3_data)
    # put_redis_data(aggregation)
    put_s3_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": redis_resource},
)

# machine_learning_job_docker = machine_learning_graph.to_job(
#     name="machine_learning_job_docker", config=docker, resource_defs={"s3": s3_resource}
# )
