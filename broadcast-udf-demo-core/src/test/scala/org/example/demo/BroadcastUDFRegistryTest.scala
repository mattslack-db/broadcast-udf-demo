package org.example.demo

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, expr, lit, sum}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.ClassTag

abstract class BroadcastUDFRegistryTest[T <: BroadcastUDFRegistry] extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = buildSparkSession()

  var udfRegistry: T = _
  var lookupDf1: DataFrame = _
  var lookupDf2: DataFrame = _
  var lookupDf3: DataFrame = _
  var expectedRows: Long = _

  import spark.implicits._

  def createRegistry(): T

  override def beforeAll: Unit = {
    lookupDf1 = Seq(
      ("key1", "value1"),
      ("key2", "value2"),
      ("key3", "value3"),
    ).toDF("col1", "col2")

    lookupDf2 = Seq(
      (1, "value1", 10),
      (2, "value2", 11),
      (3, "value3", 12),
    ).toDF("col1", "col2", "col3")

    lookupDf3 = Seq(
      (java.sql.Timestamp.valueOf("2025-10-01 10:00:00"), "value1", 10),
      (java.sql.Timestamp.valueOf("2025-10-01 11:00:00"), "value2", 11),
      (java.sql.Timestamp.valueOf("2025-10-01 12:00:00"), "value3", 12),
    ).toDF("timestampCol", "strCol", "intCol")

    udfRegistry = createRegistry()

    udfRegistry.initializeFromRows("dataset_1", seqToPythonCollect(lookupDf1))
    udfRegistry.initializeFromRows("dataset_2", seqToPythonCollect(lookupDf2), lookupDf2.schema.toDDL)
    udfRegistry.initializeFromRows("dataset_3", seqToPythonCollect(lookupDf3), lookupDf3.schema.toDDL)
    udfRegistry.updateBroadcast(spark)
    udfRegistry.registerUDFs(spark)
  }

  override def afterAll: Unit = {
    udfRegistry.cleanup()
  }

  test("A UDF should return metadata about the cache") {
    val outputDf = Seq(1L).toDF("col1").repartition(1)
      .select(expr("report_cache_metadata(col1)").alias("cache_metadata"))

    outputDf.printSchema()
    outputDf.select(explode(col("cache_metadata"))).select("col._1").show(truncate = false)

    assert(outputDf.count() == 1)
  }

  test("A UDF should return information about what is in the cache") {
    val outputDf = Seq(1L).toDF("col1").repartition(1)
      .select(expr("get_caches(col1)").alias("caches"))

    outputDf.printSchema()
    outputDf.select(explode(col("caches"))).select("col._1", "col._2").show(truncate = false)
  }

  test("A UDF should lookup values using a calculate UDF") {
    val inputDf: DataFrame = Seq(
      ("key1", 1),
      ("key2", 2),
      ("key1", 2),
      ("key3", 4),
      ("unknown", 1),
    ).toDF("col1", "col2").repartition(1)

    val resultDf = inputDf.withColumn("calculate", expr("calculate(col1, col2)"))

    val outputDf = resultDf.select("col1", "col2", "calculate.*")

    outputDf.show(truncate = false)

    assert(resultDf.columns.contains("calculate"))
    assert(outputDf.count() == inputDf.count())
  }

  test("A UDF should lookup timestamp values using a calculate UDF") {
    /*
   * This example checks doing a lookup on timestamp
   */
    val inputDf2: DataFrame = Seq(
      (java.sql.Timestamp.valueOf("2025-10-01 09:50:00"), java.sql.Timestamp.valueOf("2025-10-01 10:10:00")),
      (java.sql.Timestamp.valueOf("2025-10-01 11:50:00"), java.sql.Timestamp.valueOf("2025-10-01 12:10:00")),
      (java.sql.Timestamp.valueOf("2025-10-01 12:50:00"), java.sql.Timestamp.valueOf("2025-10-01 13:10:00"))
    ).toDF("startTimestamp", "endTimestamp").repartition(1)

    val resultDf2: DataFrame = inputDf2.withColumn("calculate", expr("calculate_timestamp(startTimestamp, endTimestamp)"))

    resultDf2.select((inputDf2.columns :+ "calculate.*").map(col): _*).show()
  }

  test("A UDF should scale up to lots of rows using a calculate UDF") {
    /*
   * This example checks that as we scale up the number of rows, the cache is still only loaded once
   */
    val inputDf = spark.range(1, 100000).toDF().repartition(100)
      .select(col("id").cast("int").alias("col2"), lit("key1").alias("col1"))

    inputDf.show()

    val resultDf = inputDf.withColumn("calculate", expr("calculate(col1, col2)"))

    assert(resultDf.select(sum(col("calculate._3"))).head().getAs[Long](0) == 10 + 11 + 12 + 3 - inputDf.count())
  }
}
