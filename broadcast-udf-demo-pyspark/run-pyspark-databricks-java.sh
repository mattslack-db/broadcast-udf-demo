#!/bin/bash
# Run the demo's Java implementation on a Databricks classic cluster.
# See run-pyspark-databricks.sh for the required environment variables.
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/run-pyspark-databricks.sh" JAVA
