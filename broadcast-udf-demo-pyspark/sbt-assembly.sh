CUR_DIR=$(pwd)

cd ../broadcast-udf-demo-core
sbt clean assembly

cd "$CUR_DIR"
