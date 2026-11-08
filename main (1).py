"""
Spark Orders Analytics.
Считает выручку по дням, топ-5 товаров, средний чек и выручку по категориям.

Запуск:
    python src/main.py --input data/orders.csv --output out
"""
import argparse
import logging

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType,
)

SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("dt", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product", StringType(), False),
    StructField("category", StringType(), False),
    StructField("qty", IntegerType(), False),
    StructField("price", DoubleType(), False),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("orders-analytics")
        .master("local[*]")
        .getOrCreate()
    )


def load_orders(spark: SparkSession, path: str):
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMA)
        .csv(path)
        .withColumn("dt", F.to_date("dt"))
        .withColumn("revenue", F.col("qty") * F.col("price"))
    )


def revenue_by_day(orders):
    return (
        orders.groupBy("dt")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy("dt")
    )


def top_products(orders, n: int = 5):
    return (
        orders.groupBy("product", "category")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
        .limit(n)
    )


def revenue_by_category(orders):
    return (
        orders.groupBy("category")
        .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
        .orderBy(F.desc("revenue"))
    )


def avg_check_by_customer(orders):
    order_totals = orders.withColumn(
        "order_revenue",
        F.sum("revenue").over(Window.partitionBy("order_id")),
    ).select("customer_id", "order_id", "order_revenue").distinct()
    return (
        order_totals.groupBy("customer_id")
        .agg(F.round(F.avg("order_revenue"), 2).alias("avg_check"))
        .orderBy(F.desc("avg_check"))
    )


def write(df, out_dir: str, name: str, fmt: str):
    target = f"{out_dir}/{name}"
    if fmt == "csv":
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(target)
    else:
        df.write.mode("overwrite").parquet(target)
    logging.info("written: %s", target)


def main():
    parser = argparse.ArgumentParser(description="Orders analytics Spark job")
    parser.add_argument("--input", required=True, help="CSV с заказами")
    parser.add_argument("--output", default="out", help="каталог результатов")
    parser.add_argument("--fmt", default="parquet", choices=["parquet", "csv"])
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders = load_orders(spark, args.input)
    orders.cache()
    logging.info("orders loaded: %s rows", orders.count())

    write(revenue_by_day(orders), args.output, "revenue_by_day", args.fmt)
    write(top_products(orders), args.output, "top_products", args.fmt)
    write(revenue_by_category(orders), args.output, "revenue_by_category", args.fmt)
    write(avg_check_by_customer(orders), args.output, "avg_check_by_customer", args.fmt)

    spark.stop()


if __name__ == "__main__":
    main()
