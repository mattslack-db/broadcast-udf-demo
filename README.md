# Broadcast UDF Demo

A small, self-contained Spark project that demonstrates how to make **reference data**
available inside **Spark SQL UDFs** efficiently — by broadcasting the data once and building a
**per-executor cache** that is initialised a single time per JVM, rather than being rebuilt or
re-serialised on every task or every row.

The same pattern is implemented **twice** — once in **Java** and once in **Scala** — so you can
compare the two approaches. A PySpark harness drives either implementation over `spark._jvm`.

- **Spark 4.1.x** · **Scala 2.13** · **Java 17**

## The problem this solves

Calculator-style UDFs often need to look things up in reference datasets (lookup tables,
configuration, small dimension tables). Two naive approaches both scale badly:

1. **Capturing the data in the UDF closure** — Spark then serialises the whole dataset into
   *every task*, defeating the point of having the data in memory.
2. **Rebuilding the lookup structures per call** — expensive work repeated for every row.

### The pattern used here

1. Reference datasets are collected on the driver and **broadcast once** to the executors.
2. The first task on each executor **builds a cache** (`ReferenceDataInput`) from the broadcast
   value, guarded by **double-checked locking** so it happens **once per executor JVM**, not once
   per task or per row.
3. UDFs read from that shared, already-built cache. Subsequent tasks on the same executor reuse
   it for free.

A 100k-row scale test in the harness asserts the cache is built only once even as the number of
tasks grows.

## Repository layout

```
broadcast-udf-demo/
├── broadcast-udf-demo-core/        # The library: reference cache + UDF registry (Java & Scala)
│   ├── pom.xml                     # Maven build (produces a thin JAR; Spark/Scala are 'provided')
│   └── src/main/
│       ├── java/org/example/       # Calculator domain classes (example calculation)
│       └── java/org/example/demo/  # BroadcastUDFRegistry (interface) + Impl (Java)
│       └── scala/org/example/demo/ # BroadcastUDFRegistryScala (Scala)
└── broadcast-udf-demo-pyspark/     # PySpark harness that exercises the library
    ├── src/python-tests.py         # Builds sample data, registers the UDFs, asserts results
    ├── mvn-assembly.sh             # Build the core JAR with Maven
    ├── run-pyspark-local-*.sh      # Run the harness against a local Spark (JAVA / SCALA)
    ├── run-pyspark-remote-*.sh     # Run against a local standalone Spark master
    └── run-all.sh                  # Convenience: run all local variants
```

## The UDFs

`registerUDFs` installs the following (identical names in the Java and Scala implementations):

| UDF | Input | Purpose |
|-----|-------|---------|
| `calculate(key, id)` | string, int | Look up `dataset_1` by key and `dataset_2` by id; returns a struct |
| `calculate_timestamp(start, end)` | timestamp, timestamp | Find the `dataset_3` row whose timestamp falls in the window |
| `calculate_row_to_tuple(struct)` | struct | Run the example calculator over a struct input; returns a struct (with an error field) |
| `report_cache_metadata(x)` | int (dummy) | Diagnostic: per-dataset row/column/schema counts, read from the broadcast |
| `get_caches(x)` | int (dummy) | Diagnostic: dump the cached rows, read from the broadcast |

## Prerequisites

- **JDK 17**
- **Maven 3.9+**
- **Apache Spark 4.1.x** (for local runs; the harness uses `spark-submit`)
- **Python 3.12+** with PySpark 4.x (see `broadcast-udf-demo-pyspark/pyproject.toml`)

## Build

The library builds with Maven into a **thin JAR** — Spark, Scala and Hadoop are `provided` scope
and excluded from the shaded artifact, so it only contains the demo's own classes and drops
cleanly onto any matching Spark runtime.

```bash
cd broadcast-udf-demo-core
mvn clean package                 # runs unit tests too
# or, from broadcast-udf-demo-pyspark/:  ./mvn-assembly.sh
```

Output: `broadcast-udf-demo-core/target/broadcast-udf-demo-core-<version>.jar`

## Run locally

From `broadcast-udf-demo-pyspark/`, with `SPARK_HOME` pointing at a Spark 4.1.x install
(the scripts default to a Homebrew path — override `SPARK_HOME` as needed):

```bash
./run-pyspark-local-java.sh       # exercise the Java implementation
./run-pyspark-local-scala.sh      # exercise the Scala implementation
./run-all.sh                      # run every local variant
```

Each script runs `python-tests.py`, which builds sample datasets, registers the UDFs against the
chosen implementation (`--mode JAVA` or `--mode SCALA`), and asserts the results — including a
100k-row scale test and an error-handling case. A clean run prints the UDF output tables and exits
0 with no assertion failures.

> The Scala UDFs require `spark.sql.legacy.allowUntypedScalaUDF=true`; the harness sets this at
> runtime.

## Run on a Databricks classic cluster

The thin JAR runs on a Databricks classic (all-purpose) cluster whose runtime matches the build
(**DBR with Spark 4.1 / Scala 2.13**, e.g. a `*-scala2.13` 18.x runtime). The cluster must use a
**Unity Catalog access mode** (e.g. single-user) — legacy no-isolation clusters cannot install
libraries from Volumes.

Use the provided scripts (from `broadcast-udf-demo-pyspark/`). They build the thin JAR, stage it
to a UC Volume, install it as a cluster library, and run the (unmodified) `python-tests.py` on the
cluster via the Databricks command execution API:

```bash
export DATABRICKS_PROFILE=<profile>            # a configured Databricks CLI profile
export DATABRICKS_CLUSTER_ID=<cluster-id>      # an existing UC single-user classic cluster
export VOLUME_DIR=/Volumes/<catalog>/<schema>/<volume>   # where to stage the JAR

./run-pyspark-databricks-java.sh               # exercise the Java implementation
./run-pyspark-databricks-scala.sh              # exercise the Scala implementation
```

Both delegate to `run-pyspark-databricks.sh <JAVA|SCALA>`; set `SKIP_BUILD=1` to reuse an
already-built JAR. The Scala UDFs need `spark.sql.legacy.allowUntypedScalaUDF=true`, which
`python-tests.py` sets at runtime.

> Because a classic cluster only loads a JAR's classes when the JVM starts, **restart the cluster**
> after replacing the JAR so the new bytes are picked up.

## License

Apache License 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
