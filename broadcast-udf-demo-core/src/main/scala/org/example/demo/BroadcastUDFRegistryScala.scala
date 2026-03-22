package org.example.demo {

  import org.example.{AnExampleClass1, AnExampleClass2, AnExampleClassFactory, CalculatorFactory, CalculatorInputObject, ReferenceDataInput}
  import org.apache.spark.TaskContext
  import org.apache.spark.broadcast.Broadcast
  import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
  import org.apache.spark.sql.functions.{typeof, udf}
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.{Row, SparkSession}

  import java.sql.Timestamp
  import scala.jdk.CollectionConverters._
  import scala.collection.mutable
  import scala.reflect.{ClassTag, classTag}

  class BroadcastUDFRegistryScala extends BroadcastUDFRegistry with java.io.Serializable {

    // This will contain one entry for every dataset that is loaded to the cache
    private val datasets = mutable.Map[String, Seq[Row]]()

    // This will contain one entry for every dataset that is loaded to the cache
    @volatile private var broadcastDatasets: Broadcast[mutable.Map[String, Seq[Row]]] = _

    /**
     * Initialize from existing collection of Rows
     *
     * It is not possible to initialize directly from the DataFrame, as then having the SparkContext for some reason causes serialisation
     * errors that do not occur with this approach. Note that this method would be nicer if it accepted Iterable[Row], but as the data is
     * coming from Python, we have to support java.util.List[java.util.List[Object]], and then convert back to Iterable[Row] using the
     * RowFactory factory class. https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/RowFactory.html
     *
     * Note would normally use ddlOpt: Option[String] but this isn't supported by Python
     */
    def initializeFromRows(dataset: String, rows: java.util.List[java.util.List[Object]], ddlOpt: String = null): Unit = {
      if (!datasets.contains(dataset)) {
        val scalaRows = rows
          .asScala
          .map(row =>
            Option(ddlOpt) match {
              case Some(ddl) => new GenericRowWithSchema(row.asScala.toArray, StructType.fromDDL(ddl))
              case _ => Row.apply(row.toArray: _*)
            })

        datasets += (dataset -> scalaRows.toSeq)
      }
    }

    /*
     * Only required by Python as it does not identify the optional parameter
     */
    def initializeFromRows(dataset: String, rows: java.util.List[java.util.List[Object]]): Unit =
      initializeFromRows(dataset, rows, null)

    def updateBroadcast(spark: SparkSession): Unit =
      broadcastDatasets = spark.sparkContext.broadcast(datasets)

    /*
     * Calculator example 1 - simple
     *
     * First check that the cache is built, then run the calculator using the cache and return the results as a Tuple
     */
    private def calculateWithFieldsToTuple(lookupDataset1: String, lookupDataset2: Int): (String, Option[String], Int) = {
      val retval1 = BroadcastUDFRegistryScala.referenceDataObject.getDataset1.asScala.find(row => row.getCol1 == lookupDataset1).map(_.getCol2).getOrElse("NOT_FOUND")
      val retval2 = BroadcastUDFRegistryScala.referenceDataObject.getDataset2.asScala.find(row => row.getCol1 == lookupDataset2).map(_.getCol2)
      val retval3 = BroadcastUDFRegistryScala.referenceDataObject.getDataset2.asScala.find(row => row.getCol1 == lookupDataset2).map(_.getCol3).getOrElse(-1)

      (retval1, retval2, retval3)
    }

    /*
     * Calculator example 2 - timestamp filter
     *
     * First check that the cache is built, then run the calculator using the cache and return the results as a Tuple
     * with Option fields, which are passed back as NULLs in the DataFrame
     */
    private def calculateWithTimestampToTuple(startTimestamp: Timestamp, endTimestamp: Timestamp): (Option[Int], Option[String]) = {
      BroadcastUDFRegistryScala.referenceDataObject.getDataset3.asScala.find(row =>
          startTimestamp.before(row.getAs[Timestamp]("timestampCol")) &&
            endTimestamp.after(row.getAs[Timestamp]("timestampCol")))
        .map(row => (Some(row.getAs[Int]("intCol")), Some(row.getAs[String]("strCol"))))
        .getOrElse((None, None))
    }

    /*
     * Calculator example 3 - UDF that takes a Row as input
     *
     * First check that the cache is built, then run the calculator using the cache, with the input fields defined
     * as a Row and return the results as a Row. In this scenario the input fields would be passed in as a struct
     * and can be accessed by name.
     */
    private def calculateWithRowToRow(input: Row): Row = {
      // this would be replaced by the actual calculate function
      BroadcastUDFRegistryScala.referenceDataObject.getDataset3.asScala.find(row =>
          input.getAs[Timestamp]("startTimestamp").before(row.getAs[Timestamp]("timestampCol")) &&
            input.getAs[Timestamp]("endTimestamp").after(row.getAs[Timestamp]("timestampCol")))
        .map(row => Row.apply(row.getAs[Int]("intCol"), row.getAs[String]("strCol")))
        .getOrElse(Row.apply(None, None))
    }

    /*
     * Calculator example 4 - UDF that takes a Row as input and returns a Tuple
     *
     * First check that the cache is built, then run the calculator using the cache, with the input fields defined
     * as a Row and return the results as a Tuple. In this scenario the input fields would be passed in as a struct
     * and can be accessed by name, the output fields will be returned as a struct and can be star expanded.
     */
    private def calculateWithRowToTuple(input: Row): (Option[Long], Option[String], Option[Double], Option[BigDecimal], Option[Timestamp], Option[String]) = {
      val factory = new CalculatorFactory(BroadcastUDFRegistryScala.referenceDataObject)

      val inputObject = new CalculatorInputObject()

      inputObject.setInputCol1(input.getAs[Long]("intCol"))
      inputObject.setInputCol2(input.getAs[String]("strCol"))
      inputObject.setInputCol3(input.getAs[Double]("doubleCol"))
      inputObject.setInputCol4(input.getDecimal(input.fieldIndex("decimalCol")).doubleValue())
      //inputObject.setInputCol4(input.getAs[java.math.BigDecimal]("decimalCol").doubleValue())
      inputObject.setInputCol5(input.getAs[Timestamp]("timestampCol"))
      // In practice there would be more fields like this

      try {
        val calculator = factory.getCalculator(inputObject)
        val objectOutput = calculator.calculate()

        // Get the values back out of the calculate output, and put them into a tuple, data types need to match
        // the return type of the function above. With this approach do not need to specify a schema on the UDF
        (Some(objectOutput.getOutputCol1), Some(objectOutput.getOutputCol2), Some(objectOutput.getOutputCol3), Some(objectOutput.getOutputCol4), Some(objectOutput.getOutputCol5), None)
      }
      catch
      {
        case ex: ArithmeticException =>
          (None, None, None, None, None, Some(ex.getMessage))
      }
    }

    private def reportCacheMetadata(): Seq[(String, Int, Option[Int], Option[String])] = {
      datasets.toSeq.map { case (k, v) =>
        (k, v.size,
          if (v.isEmpty) None else Some(v.head.size),
          if (v.isEmpty || v.head.schema == null) None else Some(v.head.schema.toDDL))
      }
    }

    private def getCaches: Seq[(String, Seq[String])] = {
      datasets.toSeq.map { case (k, v) =>
        (k, v.map { row => row.toString() })
      }
    }

    /*
     * Register the UDFs for this calculator
     *
     * Normally there would only be one of these for each calculator, there are three examples here to show different
     * ways of calling the UDF
     */
    def registerUDFs(spark: SparkSession): Unit = {
      spark.udf.register("calculate", udf((lookupDataset1: String, lookupDataset2: Int) => {
        BroadcastUDFRegistryScala.buildCache(broadcastDatasets.value)

        calculateWithFieldsToTuple(lookupDataset1, lookupDataset2)
      }))

      spark.udf.register("calculate_timestamp", udf((startTimestamp: Timestamp, endTimestamp: Timestamp) => {
        BroadcastUDFRegistryScala.buildCache(broadcastDatasets.value)

        calculateWithTimestampToTuple(startTimestamp, endTimestamp)
      })) // , StructType.fromDDL("intCol int, strCol string")

      // works, but have to set spark.sql.legacy.allowUntypedScalaUDF
      //spark.udf.register("calculate_row_to_row", udf(row => calculateWithRowToRow(row), StructType.fromDDL("intCol int, strCol string")))

      spark.udf.register("calculate_row_to_tuple", udf((row: Row) => {
        BroadcastUDFRegistryScala.buildCache(broadcastDatasets.value)

        calculateWithRowToTuple(row)
      }))

      spark.udf.register("report_cache_metadata", udf((_: Int) => {
        BroadcastUDFRegistryScala.buildCache(broadcastDatasets.value)

        reportCacheMetadata()
      }))

      spark.udf.register("get_caches", udf((_: Int) => {
        BroadcastUDFRegistryScala.buildCache(broadcastDatasets.value)

        getCaches
      }))
    }

    def cleanup(): Unit = {
      if (broadcastDatasets != null) {
        broadcastDatasets.unpersist()
      }
    }
  }

  object BroadcastUDFRegistryScala {
    /**
     * This is the variable that points to the cache which means that it is shared between tasks on the same
     * executor. In practice AnExampleCache would be replaced by the application-specific cache name
     */
    private var referenceDataObject: ReferenceDataInput = _

    /**
     * Populate the list of Java objects for this cache dataset
     *
     */
    private def buildCacheDataset[T: ClassTag](name: String, rows: Seq[Row], builder: Row => T): java.util.ArrayList[T] = {
      val list = new java.util.ArrayList[T]()

      println(s"Loading cache for $name from ${rows.length} entries into ${classTag[T]}")

      rows.foreach(row => {
        println(s"Loading row ${row.toString()}")

        list.add(builder(row))
      })

      println(s"Loaded cache for $name with ${list.size()} entries")

      assert(rows.length == list.size())

      list
    }


    /**
     * Build the cache for this pipeline
     *
     * This must only run on the executor, and should enter the lock once per executor, not once per task
     */
    private def buildCache(datasets: mutable.Map[String, Seq[Row]]): Unit =
      if (referenceDataObject == null)
        synchronized {
          if (referenceDataObject == null) {
            val task = TaskContext.get()

            println(s"Instantiating cache for partition ${if (task == null) "null" else task.partitionId()} from ${datasets.size} datasets...")

            referenceDataObject = new ReferenceDataInput()

            // This is the example we will be using with the calculator
            referenceDataObject.setDataset1(
              buildCacheDataset[AnExampleClass1]("dataset_1", datasets("dataset_1"), row => {
                AnExampleClassFactory.create(row.getAs[String](0), row.getAs[String](1))
              })
            )

            // These are other examples we won't be using
            if (datasets.contains("dataset_2")) {
              referenceDataObject.setDataset2(
                buildCacheDataset[AnExampleClass2]("dataset_2", datasets("dataset_2"), row => {
                  new AnExampleClass2(row.getAs[Int]("col1"), row.getAs[String]("col2"), row.getAs[Int]("col3"))
                })
              )
            }

            if (datasets.contains("dataset_3")) {
              referenceDataObject.setDataset3(
                buildCacheDataset[Row]("dataset_3", datasets("dataset_3"), row => row)
              )
            }

            if (task != null)
              println(s"Instantiated cache for partition ${if (task == null) "null" else task.partitionId()}")
          }
        }
  }
}