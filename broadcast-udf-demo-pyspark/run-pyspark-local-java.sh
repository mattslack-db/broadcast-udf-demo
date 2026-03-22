JAR=../broadcast-udf-demo-core/target/broadcast-udf-demo-core-0.1.8-SNAPSHOT.jar
MODE=JAVA
SPARK_HOME=${SPARK_HOME:-/opt/homebrew/Cellar/apache-spark/4.1.1/libexec}

$SPARK_HOME/bin/spark-submit --master "local[4]" --jars $JAR src/python-tests.py --mode $MODE
