from datetime import datetime
from typing import List

from dagster import (
    AssetSelection,
    Nothing,
    OpExecutionContext,
    ScheduleDefinition,
    String,
    asset,
    define_asset_job,
    load_assets_from_current_module
)
from workspaces.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": String},
    required_resource_keys={"s3"},  # use with context
    op_tags={"kind": "s3"},  # new param
    description="Get stock data from s3 bucket.",
)
def get_s3_data(context: OpExecutionContext):
    s3_key = context.op_config["s3_key"]
    # use the s3 resource key to call s3 client method get_data
    s3_lines = context.resources.s3.get_data(s3_key)
    stocks = list(map(Stock.from_list, s3_lines))
    context.log.info("Fetched s3 stocks data")
    return stocks


@asset(
    description="Filter and return highest stock (mock aggregation).",
)
def process_data(context: OpExecutionContext, get_s3_data: List[Stock]):
    # use max with lambda for highest stock
    high_stock = max(get_s3_data, key=lambda x: x.high)
    aggregation = Aggregation(date=high_stock.date, high=high_stock.high)
    context.log.info("Aggregation complete")
    return aggregation


@asset(
    required_resource_keys={"redis"},
    op_tags={"kind": "redis"},
    description="Writes to redis cache",
)
def put_redis_data(context: OpExecutionContext, process_data: Aggregation):
    name = str(process_data.date)
    value = str(process_data.high)
    # use the redis resource key to call redis client method put_data
    context.resources.redis.put_data(name, value)
    context.log.info("Aggregation written to redis cache")


@asset(
    required_resource_keys={"s3"},
    op_tags={"kind": "s3"},
    description="Aggregation written to s3",
)
def put_s3_data(context: OpExecutionContext, process_data: Aggregation):
    key_name = str(process_data.date)
    # use the s3 resource key to call the client method put_data
    context.resources.s3.put_data(key_name, process_data)
    context.log.info("Aggregation written to S3")


# create a group namespace
# these will now be unpacked in Definition
project_assets = load_assets_from_current_module(
    group_name="week_4_assets",
)

machine_learning_asset_job = define_asset_job(
    name="machine_learning_asset_job",
    selection=AssetSelection.groups("week_4_assets"),
    # set config on one stock
    config={
         "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}
    }
)

machine_learning_schedule = ScheduleDefinition(
    job=machine_learning_asset_job, cron_schedule="*/15 * * * *")
