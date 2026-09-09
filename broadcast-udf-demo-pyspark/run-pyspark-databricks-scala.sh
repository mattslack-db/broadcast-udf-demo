#!/bin/bash
# Run the demo's Scala implementation on a Databricks classic cluster.
# See run-pyspark-databricks.sh for the required environment variables.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-pyspark-databricks.sh" SCALA
