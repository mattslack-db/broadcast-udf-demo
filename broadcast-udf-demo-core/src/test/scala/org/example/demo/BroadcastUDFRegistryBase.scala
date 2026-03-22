package org.example.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

abstract class BroadcastUDFRegistryBase[T <: BroadcastUDFRegistry] extends AnyFunSuite with BeforeAndAfterAll {

  val spark: SparkSession = buildSparkSession()

  var udfRegistry: BroadcastUDFRegistry = _
  var lookupDf: DataFrame = _

  import spark.implicits._

  def createRegistry(): T

  override def beforeAll: Unit = {
    lookupDf = Seq(
      ("key1", "value1"),
      ("key2", "value2"),
      ("key3", "value3"),
    ).toDF("col1", "col2")

    udfRegistry = createRegistry()

    udfRegistry.initializeFromRows("dataset_1", seqToPythonCollect(lookupDf), lookupDf.schema.toDDL)
    udfRegistry.updateBroadcast(spark)
    udfRegistry.registerUDFs(spark)
  }

  override def afterAll: Unit = {
    udfRegistry.cleanup()
  }

  test("A UDF should lookup values using a calculate UDF") {
    /*
     * This example shows how to input a struct as a Row, i.e. so don't have to predefine the input data types when
     * defining the UDF
     */
    val schema = StructType(Seq(
      StructField("intCol", LongType, nullable = false),
      StructField("strCol", StringType, nullable = false),
      StructField("doubleCol", DoubleType, nullable = true),
      StructField("decimalCol", DecimalType(38, 18), nullable = false),  // Adjust precision/scale as needed
      StructField("timestampCol", TimestampType, nullable = false)
    ))

    val data = Seq(
      Row(1L, "value1", 4.0, BigDecimal("1.0").bigDecimal, Timestamp.valueOf("2025-10-01 10:00:00")),
      Row(2L, "value2", null, BigDecimal("2.0").bigDecimal, Timestamp.valueOf("2025-10-01 11:00:00")),
      Row(3L, "value3", -1.0, BigDecimal("3.0").bigDecimal, Timestamp.valueOf("2025-10-01 12:00:00"))
    )

    val inputDf = spark.createDataFrame(
        spark.sparkContext.parallelize(data),
        schema
      )
      .repartition(1)

    val expectedRows = inputDf.count()
    val passedRows = expectedRows - 1

    val resultDf = inputDf.withColumn("calculate", expr("calculate_row_to_tuple(struct(intCol, strCol, doubleCol, decimalCol, timestampCol))"))

    resultDf.show(truncate = false)

    assert(resultDf.count() == expectedRows)
    assert(resultDf.columns.contains("calculate"))
    assert(resultDf.drop("calculate").columns sameElements inputDf.columns)
    assert(resultDf.select("calculate.*").columns sameElements Array("_1", "_2", "_3", "_4", "_5", "_6"))

    val passDf = resultDf.filter("calculate._6 IS NULL")
    val errorDf = resultDf.filter("calculate._6 IS NOT NULL")

    assert(passDf.count() == passedRows)
    assert(passDf.filter("intCol + 1 = calculate._1").count() == passedRows)
    assert(passDf.filter("CONCAT(strCol, ' (output)') = calculate._2").count() == passedRows)
    assert(passDf.filter("SQRT(doubleCol) = calculate._3 OR doubleCol IS NULL").count() == passedRows)
    assert(passDf.filter("decimalCol * 2 = calculate._4").count() == passedRows)
    assert(passDf.filter("timestampCol + INTERVAL 10 SECONDS = calculate._5").count() == passedRows)

    assert(errorDf.count() == 1)
    assert(errorDf.select("calculate._6").head().getAs[String](0) == "Cannot take square root of negative number")
  }
}
