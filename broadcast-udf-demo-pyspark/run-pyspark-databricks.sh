#!/bin/bash
#
# Run the broadcast UDF demo (src/python-tests.py) on a Databricks *classic* (all-purpose) cluster.
#
# Unlike run-pyspark-local-*.sh (which spark-submit to a local Spark) and run-pyspark-remote-*.sh
# (which use a local standalone Spark master), this:
#   1. builds the thin JAR,
#   2. stages it to a Unity Catalog Volume,
#   3. installs it as a library on an existing UC-enabled classic cluster,
#   4. runs python-tests.py on that cluster via the Databricks command execution API.
#
# The real python-tests.py is run unmodified: we inject sys.argv so its own argparse picks up
# --mode, keeping a single source of truth for the demo logic.
#
# Required environment variables:
#   DATABRICKS_PROFILE     Databricks CLI profile (workspace + auth)
#   DATABRICKS_CLUSTER_ID  ID of an existing classic cluster in UC single-user access mode
#                          (legacy no-isolation clusters cannot install libraries from Volumes)
#   VOLUME_DIR             UC Volume directory to stage the JAR, e.g.
#                          /Volumes/<catalog>/<schema>/<volume>
# Optional:
#   SKIP_BUILD=1           Reuse the already-built JAR instead of running mvn
#
# Usage: ./run-pyspark-databricks.sh <JAVA|SCALA>

set -euo pipefail

MODE="${1:?Usage: run-pyspark-databricks.sh <JAVA|SCALA>}"
: "${DATABRICKS_PROFILE:?set DATABRICKS_PROFILE}"
: "${DATABRICKS_CLUSTER_ID:?set DATABRICKS_CLUSTER_ID}"
: "${VOLUME_DIR:?set VOLUME_DIR (e.g. /Volumes/<catalog>/<schema>/<volume>)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$SCRIPT_DIR/../broadcast-udf-demo-core"
JAR_NAME="broadcast-udf-demo-core-0.1.8-SNAPSHOT.jar"
JAR="$CORE_DIR/target/$JAR_NAME"
PYFILE="$SCRIPT_DIR/src/python-tests.py"
PROFILE="$DATABRICKS_PROFILE"
CID="$DATABRICKS_CLUSTER_ID"
JAR_PATH="$VOLUME_DIR/$JAR_NAME"

# 1. Build the thin JAR (unless reusing an existing build)
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  ( cd "$CORE_DIR" && mvn -q clean package -DskipTests )
fi

# 2. Stage the JAR to the Volume
echo "Uploading $JAR_NAME to $VOLUME_DIR ..."
databricks fs cp "$JAR" "dbfs:$JAR_PATH" --overwrite --profile "$PROFILE"

# 3. Install it as a cluster library (idempotent) and wait until INSTALLED.
#    NOTE: a classic cluster only loads a JAR's classes when its JVM starts, so if you have
#    replaced the JAR you must restart the cluster for the new bytes to take effect.
databricks libraries install --profile "$PROFILE" \
  --json "{\"cluster_id\":\"$CID\",\"libraries\":[{\"jar\":\"$JAR_PATH\"}]}"

echo "Waiting for library to install on $CID ..."
for _ in $(seq 1 60); do
  status=$(databricks libraries cluster-status "$CID" --profile "$PROFILE" -o json 2>/dev/null \
           | jq -r --arg j "$JAR_PATH" '(if type=="array" then . else .library_statuses end)[]? | select((.library.jar // "")==$j) | .status')
  echo "  library status: ${status:-unknown}"
  [ "$status" = "INSTALLED" ] && break
  [ "$status" = "FAILED" ] && { echo "Library install FAILED"; exit 1; }
  sleep 10
done

# 4. Run python-tests.py on the cluster via the command execution API. Inject sys.argv so the
#    file's own argparse picks up --mode, running the unmodified script.
CTX=$(databricks api post /api/1.2/contexts/create --profile "$PROFILE" \
      --json "{\"clusterId\":\"$CID\",\"language\":\"python\"}" | jq -r '.id')

PAYLOAD=$(jq -n --arg cid "$CID" --arg ctx "$CTX" --arg mode "$MODE" --rawfile code "$PYFILE" \
  '{clusterId:$cid, contextId:$ctx, language:"python",
    command: ("import sys\nsys.argv = [\"python-tests.py\", \"--mode\", \"" + $mode + "\"]\n" + $code)}')
CMD=$(databricks api post /api/1.2/commands/execute --profile "$PROFILE" --json "$PAYLOAD" | jq -r '.id')

echo "Running python-tests.py --mode $MODE on cluster $CID ..."
while :; do
  RES=$(databricks api get "/api/1.2/commands/status?clusterId=$CID&contextId=$CTX&commandId=$CMD" --profile "$PROFILE")
  STATUS=$(echo "$RES" | jq -r '.status')
  case "$STATUS" in Finished|Error|Cancelled) break ;; esac
  sleep 5
done

echo "----------------------------------------------------------------"
echo "$RES" | jq -r '.results | if .resultType=="text" then .data elif .resultType=="error" then ("ERROR:\n"+(.summary//"")+"\n"+(.cause//"")) else tostring end'
echo "----------------------------------------------------------------"

# Clean up the execution context
databricks api post /api/1.2/contexts/destroy --profile "$PROFILE" \
  --json "{\"clusterId\":\"$CID\",\"contextId\":\"$CTX\"}" >/dev/null 2>&1 || true

if [ "$STATUS" = "Finished" ]; then
  echo "SUCCESS (mode=$MODE)"
else
  echo "FAILED (mode=$MODE): $STATUS"
  exit 1
fi
