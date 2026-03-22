package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.jdk.CollectionConverters._
import java.util

package object demo {

  def seqToPythonCollect(df: DataFrame): util.List[util.List[AnyRef]] =
    df.collect().map(r => r.toSeq.map(_.asInstanceOf[AnyRef]).asJava).toSeq.asJava

  def buildSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession.builder.appName("BroadcastUDFDemo").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")

    spark
  }

}
