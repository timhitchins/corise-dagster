from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    String,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    static_partitioned_config,
)
from workspaces.config import REDIS, S3
from workspaces.project.sensors import get_s3_keys
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
    description="Aggregation written to s3",
)
def put_s3_data(context: OpExecutionContext, aggregation):
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


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

# partition config for static items
@static_partitioned_config(partition_keys=[str(i) for i in range(1, 11, 1)])
def docker_config(partition_key: str):
    return {
        "resources": {
            "s3": {"config": S3},
            "redis": {"config": REDIS},
        },
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }


machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    # use mocks
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    # 10 retires with a delay of 1 secornd
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
    # use partitioned docker config
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)

# Every 15 mins fire configured machine_learning_job_docker
machine_learning_schedule_local = ScheduleDefinition(job=machine_learning_job_local, cron_schedule="*/15 * * * *")

# Every hour fire configured machine_learning_job_docker
@schedule(cron_schedule="0 * * * *", job=machine_learning_job_docker)
def machine_learning_schedule_docker(context):
    for partition_key in docker_config.get_partition_keys():
        # Yield a run request with partition configs
        context.log.info(f"Runnng shceduled job for {partition_key}")
        yield RunRequest(
            run_key=partition_key,
            # use config defined in docker config
            run_config=docker_config.get_run_config_for_partition_key(partition_key),
        )


@sensor(job=machine_learning_job_docker, minimum_interval_seconds=30)
def machine_learning_sensor_docker(context: SensorEvaluationContext):
    s3_keys = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url="http://localstack:4566")

    if not s3_keys:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for s3_key in s3_keys:
        context.log.info(f"Runnng sensor job for {s3_key}")
        yield RunRequest(
            run_key=s3_key,
            run_config={
                # pass in the resources required for the job
                "resources": {
                    "s3": {"config": S3},
                    "redis": {"config": REDIS},
                },
                "ops": {
                    "get_s3_data": {"config": {"s3_key": s3_key}},
                },
            },
        )
