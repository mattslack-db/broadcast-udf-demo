HOST=$(hostname)
PORT=7077
SPARK_URL=spark://$HOST:$PORT
JAR=../broadcast-udf-demo-core/target/broadcast-udf-demo-core-0.1.8-SNAPSHOT.jar
MODE=JAVA
SPARK_HOME=${SPARK_HOME:-/opt/homebrew/Cellar/apache-spark/4.1.1/libexec}

$SPARK_HOME/sbin/start-master.sh --host $HOST
$SPARK_HOME/sbin/start-worker.sh $SPARK_URL
$SPARK_HOME/bin/spark-submit --master $SPARK_URL --jars $JAR src/python-tests.py --mode $MODE
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh
