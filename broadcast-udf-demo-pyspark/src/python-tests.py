from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, lit, sum
from datetime import datetime
from argparse import ArgumentParser
from concurrent.futures import ThreadPoolExecutor

import os
from decimal import Context

os.environ.setdefault('PYSPARK_PYTHON', 'python3')
os.environ.setdefault('PYSPARK_DRIVER_PYTHON', 'python3')

parser = ArgumentParser()
parser.add_argument("-m", "--mode", dest="mode", help="Java or Python", choices=['JAVA', 'SCALA'])

args = parser.parse_args()

spark = SparkSession.builder.appName('BroadcastUDFDemo').getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

lookupDf1 = spark.createDataFrame([
    ("key1", "value1"),
    ("key2", "value2"),
    ("key3", "value3"),
], ["col1", "col2"])
lookupDf1.createOrReplaceTempView("dataset_1")

lookupDf2 = spark.createDataFrame([
    (1, "value1", 10),
    (2, "value2", 11),
    (3, "value3", 12),
], ["col1", "col2", "col3"])
lookupDf2.createOrReplaceTempView("dataset_2")

lookupDf3 = spark.createDataFrame([
    (datetime.fromisoformat("2025-10-01 10:00:00"), "value1", 10),
    (datetime.fromisoformat("2025-10-01 11:00:00"), "value2", 11),
    (datetime.fromisoformat("2025-10-01 12:00:00"), "value3", 12)
], ["timestampCol", "strCol", "intCol"])
lookupDf3.createOrReplaceTempView("dataset_3")

lookups = {
    "dataset_1": {"sql": "SELECT * FROM dataset_1" },
    "dataset_2": {"sql": "SELECT * FROM dataset_2", "ddl": lookupDf2.schema.toDDL()},
    "dataset_3": {"sql": "SELECT * FROM dataset_3", "ddl": lookupDf3.schema.toDDL()}
}

if (args.mode == "JAVA"):
    print("Instantiating org.example.demo.BroadcastUDFRegistryImpl")
    udfRegistry = spark._jvm.org.example.demo.BroadcastUDFRegistryImpl()
else:
    print("Instantiating org.example.demo.BroadcastUDFRegistryScala")
    udfRegistry = spark._jvm.org.example.demo.BroadcastUDFRegistryScala()

# Helper function for parallel initialization
def initialize_dataset(dataset, lookup):
    udfRegistry.initializeFromRows(dataset, spark.sql(lookup["sql"]).collect(), lookup.get("ddl"))

# Run initializations in parallel. Wrap in list(...) so the lazy map is fully consumed and
# any exception raised inside initialize_dataset is propagated instead of silently swallowed.
with ThreadPoolExecutor() as executor:
    list(executor.map(lambda item: initialize_dataset(item[0], item[1]), lookups.items()))

#udfRegistry.initializeFromRows("dataset_1", lookupDf1.collect())
#udfRegistry.initializeFromRows("dataset_2", lookupDf2.collect(), lookupDf2._jdf.schema().toDDL())
#dfRegistry.initializeFromRows("dataset_3", lookupDf3.collect(), lookupDf3._jdf.schema().toDDL())
udfRegistry.updateBroadcast(spark._jsparkSession)
udfRegistry.registerUDFs(spark._jsparkSession)

outputDf = spark.createDataFrame([(1,)], ["col1"]).repartition(1).select(expr("report_cache_metadata(col1)").alias("cache_metadata"))
outputDf.select(explode(col("cache_metadata"))).select("col.*").show(truncate=False)

outputDf.show(truncate=False)

outputDf = spark.createDataFrame([(1,)], ["col1"]).repartition(1).select(expr("get_caches(col1)").alias("caches"))
outputDf.select(explode(col("caches"))).select("col.*").show(truncate=False)

outputDf.show(truncate=False)

inputDf1 = spark.createDataFrame([
    ("key1", 1),
    ("key2", 2),
    ("key3", 4),
    ("unknown", 1),
], ["col1", "col2"]).withColumn("col2", col("col2").cast("int")).repartition(2)

resultDf1 = inputDf1.withColumn("calculate", expr("calculate(col1, col2)"))

resultDf1.select("col1", "col2", "calculate.*").show()

#
# This example checks doing a lookup on timestamp
#
inputDf2 = spark.createDataFrame([
    (datetime.fromisoformat("2025-10-01 09:50:00"), datetime.fromisoformat("2025-10-01 10:10:00")),
    (datetime.fromisoformat("2025-10-01 11:50:00"), datetime.fromisoformat("2025-10-01 12:10:00")),
    (datetime.fromisoformat("2025-10-01 12:50:00"), datetime.fromisoformat("2025-10-01 13:10:00"))
], ["startTimestamp", "endTimestamp"]).repartition(2)

resultDf2 = inputDf2.withColumn("calculate", expr("calculate_timestamp(startTimestamp, endTimestamp)"))

resultDf2.select(inputDf2.columns + ["calculate.*"]).show()

#
# This example checks that as we scale up the number of rows, the cache is still only loaded once
#
inputDf3 = spark.range(100000).repartition(100) \
    .select(col("id").cast("int").alias("col2"), lit("key1").alias("col1"))

resultDf3 = inputDf3.withColumn("calculate", expr("calculate(col1, col2)"))

assert resultDf3.select(sum(col("calculate._3"))).head()[0] == 10 + 11 + 12 + 3 - inputDf3.count()

#
# This example shows how to input a struct as a Row, i.e. so don't have to predefine the input data types when
# defining the UDF
#
#resultDf4 = inputDf2.withColumn("calculate", expr("calculate_row_to_row(struct(startTimestamp, endTimestamp))"))

#resultDf4.select(inputDf2.columns + ["calculate.*"]).show()
def asDecimal(input: str):
    return Context(prec=38).create_decimal(input)


inputDf5 = spark.createDataFrame([
    (1, "value1", 4.0, asDecimal("1.0"), datetime.fromisoformat("2025-10-01 10:00:00")),
    (2, "value2", 9.0, asDecimal("2.0"), datetime.fromisoformat("2025-10-01 11:00:00")),
    (3, "value3", -1.0, asDecimal("3.0"), datetime.fromisoformat("2025-10-01 12:00:00")),
], ["intCol", "strCol", "doubleCol", "decimalCol", "timestampCol"]).repartition(2)

expectedRows = inputDf5.count()
passedRows = expectedRows - 1

resultDf5 = inputDf5.withColumn("calculate", expr("calculate_row_to_tuple(struct(intCol, strCol, doubleCol, decimalCol, timestampCol))"))

resultDf5.show(truncate=False)

assert resultDf5.count() == expectedRows
assert "calculate" in resultDf5.columns
assert resultDf5.drop("calculate").columns == inputDf5.columns
assert resultDf5.select("calculate.*").columns == ["_1", "_2", "_3", "_4", "_5", "_6"]

passDf = resultDf5.filter("calculate._6 IS NULL")
errorDf = resultDf5.filter("calculate._6 IS NOT NULL")

assert passDf.count() == passedRows
assert passDf.filter("intCol + 1 = calculate._1").count() == passedRows
assert passDf.filter("CONCAT(strCol, ' (output)') = calculate._2").count() == passedRows
assert passDf.filter("SQRT(doubleCol) = calculate._3").count() == passedRows
assert passDf.filter("decimalCol * 2 = calculate._4").count() == passedRows
assert passDf.filter("timestampCol + INTERVAL 10 SECONDS = calculate._5").count() == passedRows

assert errorDf.count() == 1
assert errorDf.select("calculate._6").head()[0] == "Cannot take square root of negative number"

udfRegistry.cleanup()