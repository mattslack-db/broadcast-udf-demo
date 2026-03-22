package org.example.demo;

import org.example.*;
import org.apache.spark.TaskContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple6;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BroadcastUDFRegistryImpl extends BroadcastUDFRegistry {

    // This will contain one entry for every dataset that is loaded to the cache
    private final Map<String, List<Row>> datasets = new ConcurrentHashMap<>();

    // This will contain one entry for every dataset that is loaded to the cache
    private volatile Broadcast<Map<String, List<Row>>> broadcastDatasets;

    // Static reference data object shared between tasks on the same executor
    private static volatile ReferenceDataInput staticReferenceDataObject;

    /**
     * Initialize from existing collection of Rows
     * <p>
     * It is not possible to initialize directly from the DataFrame, as then having the SparkContext for some reason causes serialisation
     * errors that do not occur with this approach. Note that this method would be nicer if it accepted Iterable[Row], but as the data is
     * coming from Python, we have to support java.util.List[java.util.List[Object]], and then convert back to Iterable[Row] using the
     * RowFactory factory class. https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/RowFactory.html
     */
    @Override
    public void initializeFromRows(String dataset, List<List<Object>> rows, String ddlOpt) {
        if (!datasets.containsKey(dataset)) {
            List<Row> scalaRows = rows.stream().map(row -> {
                if (ddlOpt != null) {
                    return new GenericRowWithSchema(row.toArray(), StructType.fromDDL(ddlOpt));
                } else {
                    return RowFactory.create(row.toArray());
                }
            }).collect(Collectors.toList());

            datasets.put(dataset, scalaRows);
        }
    }

    /**
     * Only required by Python as it does not identify the optional parameter
     */
    @Override
    public void initializeFromRows(String dataset, List<List<Object>> rows) {
        initializeFromRows(dataset, rows, null);
    }

    @Override
    public void updateBroadcast(SparkSession spark) {
        broadcastDatasets = spark.sparkContext().broadcast(datasets, scala.reflect.ClassTag$.MODULE$.apply(Map.class));
    }

    /**
     * Calculator example 1 - simple
     * <p>
     * First check that the cache is built, then run the calculator using the cache and return the results as a Tuple
     */
    private Tuple3<String, String, Integer> calculateWithFieldsToTuple(ReferenceDataInput referenceDataObject, String lookupDataset1, Integer lookupDataset2) {
        if (referenceDataObject.getDataset1() == null) {
            throw new RuntimeException("dataset1 not defined");
        }

        if (referenceDataObject.getDataset2() == null) {
            throw new RuntimeException("dataset2 not defined");
        }

        if (referenceDataObject.getDataset3() == null) {
            throw new RuntimeException("dataset3 not defined");
        }

        Stream<AnExampleClass1> stream1 = referenceDataObject.getDataset1().stream();
        Stream<AnExampleClass2> stream2 = referenceDataObject.getDataset2().stream();
        Stream<AnExampleClass2> stream3 = referenceDataObject.getDataset2().stream();

        String retval1 = stream1.filter(row -> row.getCol1().equals(lookupDataset1)).map(AnExampleClass1::getCol2).findFirst().orElse("NOT_FOUND");

        String retval2 = stream2.filter(row -> row.getCol1() == lookupDataset2).map(AnExampleClass2::getCol2).findFirst().orElse(null);

        Integer retval3 = stream3.filter(row -> row.getCol1() == lookupDataset2).map(AnExampleClass2::getCol3).findFirst().orElse(-1);

        return new Tuple3<>(retval1, retval2, retval3);
    }

    /**
     * Calculator example 2 - timestamp filter
     * <p>
     * First check that the cache is built, then run the calculator using the cache and return the results as a Tuple
     * with Option fields, which are passed back as NULLs in the DataFrame
     */
    private Tuple2<Integer, String> calculateWithTimestampToTuple(ReferenceDataInput referenceDataObject, Timestamp startTimestamp, Timestamp endTimestamp) {
        return referenceDataObject.getDataset3().stream().filter(row -> startTimestamp.before(row.getAs("timestampCol")) && endTimestamp.after(row.getAs("timestampCol"))).map(row -> new Tuple2<>(row.<Integer>getAs("intCol"), row.<String>getAs("strCol"))).findFirst().orElse(new Tuple2<>(null, null));
    }

    /**
     * Calculator example 3 - UDF that takes a Row as input
     * <p>
     * First check that the cache is built, then run the calculator using the cache, with the input fields defined
     * as a Row and return the results as a Row. In this scenario the input fields would be passed in as a struct
     * and can be accessed by name.
     */
    private Row calculateWithRowToRow(ReferenceDataInput referenceDataObject, Row input) {
        return referenceDataObject.getDataset3().stream().filter(row -> input.<Timestamp>getAs("startTimestamp").before(row.getAs("timestampCol")) && input.<Timestamp>getAs("endTimestamp").after(row.getAs("timestampCol"))).map(row -> RowFactory.create(row.<Integer>getAs("intCol"), row.<String>getAs("strCol"))).findFirst().orElse(RowFactory.create(null, null));
    }

    /**
     * Calculator example 4 - UDF that takes a Row as input and returns a Tuple
     * <p>
     * First check that the cache is built, then run the calculator using the cache, with the input fields defined
     * as a Row and return the results as a Tuple. In this scenario the input fields would be passed in as a struct
     * and can be accessed by name, the output fields will be returned as a struct and can be star expanded.
     */
    private Tuple6<Long, String, Double, BigDecimal, Timestamp, String> calculateWithRowToTuple(ReferenceDataInput referenceDataObject, Row input) {
        CalculatorFactory factory = new CalculatorFactory(referenceDataObject);

        CalculatorInputObject inputObject = new CalculatorInputObject();

        double x;

        if (input.isNullAt(2)) {
            x = 0.0;
        } else {
            x = input.getAs(2);
        }

        inputObject.setInputCol1(input.getAs("intCol"));
        inputObject.setInputCol2(input.getAs("strCol"));
        inputObject.setInputCol3(x);
        inputObject.setInputCol4(((BigDecimal) input.getAs("decimalCol")).doubleValue());
        inputObject.setInputCol5(input.getAs("timestampCol"));
        // In practice there would be more fields like this

        try {
            Calculator calculator = factory.getCalculator(inputObject);
            CalculatorOutputObject objectOutput = calculator.calculate();

            // Get the values back out of the calculate output, and put them into a tuple, data types need to match
            // the return type of the function above. With this approach do not need to specify a schema on the UDF
            return new Tuple6<>(objectOutput.getOutputCol1(), objectOutput.getOutputCol2(), objectOutput.getOutputCol3(), BigDecimal.valueOf(objectOutput.getOutputCol4()), objectOutput.getOutputCol5(), null);
        } catch (ArithmeticException ex) {
            return new Tuple6<>(null, null, null, null, null, ex.getMessage());
        }
    }

    private List<Tuple4<String, Integer, Integer, String>> reportCacheMetadata() {
        return datasets.entrySet().stream().map(entry -> {
            String key = entry.getKey();
            List<Row> value = entry.getValue();
            Integer size = value.size();
            Integer headSize = value.isEmpty() ? null : value.get(0).size();
            String schema = (value.isEmpty() || value.get(0).schema() == null) ? null : value.get(0).schema().toDDL();
            return new Tuple4<>(key, size, headSize, schema);
        }).collect(Collectors.toList());
    }

    private List<Tuple2<String, List<String>>> getCaches() {
        return datasets.entrySet().stream().map(entry -> {
            String key = entry.getKey();
            List<String> value = entry.getValue().stream()
                    .map(Row::toString)
                    .collect(Collectors.toList());

            return new Tuple2<>(key, value);
        }).collect(Collectors.toList());
    }

    private StructType getSchema1() {
        return DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("_1", DataTypes.StringType, false),
                        DataTypes.createStructField("_2", DataTypes.StringType, true),
                        DataTypes.createStructField("_3", DataTypes.IntegerType, false)});
    }

    private StructType getSchema2() {
        return DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("_1", DataTypes.IntegerType, true),
                        DataTypes.createStructField("_2", DataTypes.StringType, true)});
    }

    private StructType getSchema3() {
        return DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("_1", DataTypes.LongType, false),
                        DataTypes.createStructField("_2", DataTypes.StringType, false),
                        DataTypes.createStructField("_3", DataTypes.DoubleType, false),
                        DataTypes.createStructField("_4", DataTypes.createDecimalType(), false),
                        DataTypes.createStructField("_5", DataTypes.TimestampType, false),
                        DataTypes.createStructField("_6", DataTypes.StringType, false)});
    }

    private ArrayType getSchema4() {
        return DataTypes.createArrayType(
                DataTypes.createStructType(
                        new StructField[]{
                                DataTypes.createStructField("_1", DataTypes.StringType, false),
                                DataTypes.createStructField("_2", DataTypes.IntegerType, false),
                                DataTypes.createStructField("_3", DataTypes.IntegerType, true),
                                DataTypes.createStructField("_4", DataTypes.StringType, true)}));
    }

    /**
     * Register the UDFs for this calculator
     * <p>
     * Normally there would only be one of these for each calculator, there are three examples here to show different
     * ways of calling the UDF
     */
    @Override
    public void registerUDFs(SparkSession spark) {
        spark.udf().register("calculate", (UDF2<String, Integer, Tuple3<String, String, Integer>>) (lookupDataset1, lookupDataset2) -> {
            ReferenceDataInput referenceDataObject = buildCache(broadcastDatasets.value());
            return calculateWithFieldsToTuple(referenceDataObject, lookupDataset1, lookupDataset2);
        }, StructType.fromDDL("_1 STRING, _2 STRING, _3 INTEGER"));

        spark.udf().register("calculate_timestamp", (UDF2<Timestamp, Timestamp, Tuple2<Integer, String>>) (startTimestamp, endTimestamp) -> {
            ReferenceDataInput referenceDataObject = buildCache(broadcastDatasets.value());
            return calculateWithTimestampToTuple(referenceDataObject, startTimestamp, endTimestamp);
        }, StructType.fromDDL("_1 INTEGER, _2 STRING"));

        spark.udf().register("calculate_row_to_tuple", (UDF1<Row, Tuple6<Long, String, Double, BigDecimal, Timestamp, String>>) row -> {
            ReferenceDataInput referenceDataObject = buildCache(broadcastDatasets.value());
            return calculateWithRowToTuple(referenceDataObject, row);
        }, StructType.fromDDL("_1 LONG, _2 STRING, _3 DOUBLE, _4 DECIMAL, _5 TIMESTAMP, _6 STRING"));

        spark.udf().register("report_cache_metadata",
                (UDF1<Long, List<Tuple4<String, Integer, Integer, String>>>) dummy -> reportCacheMetadata(),
                ArrayType.fromDDL("ARRAY<STRUCT<_1 STRING, _2 INT, _3 INT, _4 STRING>>"));

        spark.udf().register("get_caches",
                (UDF1<Long, List<Tuple2<String, List<String>>>>) dummy -> getCaches(),
                ArrayType.fromDDL("ARRAY<STRUCT<_1 STRING, _2 ARRAY<STRING>>>"));
    }

    @Override
    public void cleanup() {
        if (broadcastDatasets != null) {
            broadcastDatasets.unpersist();
        }
    }

    /**
     * Build the cache for this pipeline
     * <p>
     * This must only run on the executor, and should enter the lock once per executor, not once per task
     */
    private static ReferenceDataInput buildCache(Map<String, List<Row>> datasets) {
        if (staticReferenceDataObject == null) {
            synchronized (BroadcastUDFRegistryImpl.class) {
                if (staticReferenceDataObject == null) {
                    TaskContext task = TaskContext.get();

                    if (task != null) {
                        System.out.println("Instantiating cache for partition " + task.partitionId() + "...");
                    }

                    staticReferenceDataObject = new ReferenceDataInput();

                    List<AnExampleClass1> list1 = new ArrayList<>();

                    // This is the example we will be using with the calculator
                    datasets.get("dataset_1").forEach(row -> list1.add(AnExampleClassFactory.create(row.getAs(0), row.getAs(1))));

                    staticReferenceDataObject.setDataset1(list1);

                    // These are other examples we won't be using
                    if (datasets.containsKey("dataset_2")) {
                        List<AnExampleClass2> dataset2 = datasets.get("dataset_2").stream().map(row -> new AnExampleClass2(row.getAs("col1"), row.getAs("col2"), row.getAs("col3"))).collect(Collectors.toList());

                        staticReferenceDataObject.setDataset2(dataset2);
                    }

                    if (datasets.containsKey("dataset_3")) {
                        List<Row> dataset3 = datasets.get("dataset_3");
                        staticReferenceDataObject.setDataset3(dataset3);
                    }
                }
            }
        }

        return staticReferenceDataObject;
    }
}