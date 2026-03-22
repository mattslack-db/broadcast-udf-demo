CUR_DIR=$(pwd)

cd ../broadcast-udf-demo-core
mvn clean package -f pom.xml

cd "$CUR_DIR"
