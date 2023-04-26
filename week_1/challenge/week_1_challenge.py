import csv
from datetime import datetime
from heapq import nlargest
from typing import Iterator, List

from dagster import (
    Any,
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    OpExecutionContext,
    Out,
    Output,
    String,
    job,
    op,
    usable_as_dagster_type,
)
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[str]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    out={
        "empty_stocks": Out(is_required=False),
        "stocks": Out(dagster_type=List[Stock], is_required=False),
    },
    description="Get stocks data from s3.",
)
def get_s3_data_op(context):
    s3_key = context.op_config["s3_key"]
    stocks = list(csv_helper(s3_key))
    if not stocks:
        yield Output(stocks, "empty_stocks")
    else:
        yield Output(stocks, "stocks")


@op(
    config_schema={"nlargest": int},
    ins={"stocks": In(dagster_type=List[Stock])},
    out=DynamicOut(),
    description="Dymanic nlargest stocks.",
)
def process_data_op(context, stocks) -> Aggregation:
    n = context.op_config["nlargest"]
    aggregation = nlargest(n=n, iterable=stocks, key=lambda x: x.high)
    for i, agg in enumerate(aggregation):
        yield DynamicOutput(agg, mapping_key=str(i))


@op(
    ins={"aggregation": In(dagster_type=Aggregation, description="Output of process data.")},
    out=Out(Nothing),
    tags={"kind": "redis"},
    description="Upload aggregation to redis.",
)
def put_redis_data_op(context, aggregation) -> Nothing:
    pass


@op(
    ins={"aggregation": In(dagster_type=Aggregation, description="Output of process data.")},
    out=Out(Nothing),
    tags={"kind": "s3"},
    description="Upload aggregation to s3.",
)
def put_s3_data_op(context, aggregation) -> Nothing:
    pass


@op(
    ins={"empty_stocks": In(dagster_type=Any)},
    out=Out(Nothing),
    description="Notifiy if stock list is empty",
)
def empty_stock_notify_op(context: OpExecutionContext, empty_stocks: Any):
    context.log.info("No stocks returned")


@job
def machine_learning_dynamic_job():
    empty_stocks, stocks = get_s3_data_op()
    empty_stock_notify_op(empty_stocks)
    aggregation = process_data_op(stocks)
    aggregation.map(put_redis_data_op)
    # aggregation.map(put_s3_data_op)
    # put_redis_data_op(aggregation)
    # put_s3_data_op(aggregation)
