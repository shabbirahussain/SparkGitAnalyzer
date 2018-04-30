
SCALA_BIN_PATH=
SPARK_BIN_PATH=
RUNTIME_JARS=commons-csv-1.5.jar


# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=target/artifacts/task.jar
LIB_PATH=target/dependency

COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}/$(subst ${COMMA},${COMMA}${LIB_PATH}/,${RUNTIME_JARS})

all: setup build run

build_run: build run

build:
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	${SCALA_BIN_PATH}scalac -cp "./${LIB_PATH}/*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	cp -r src/main/resources/* target/classes
	cp src/main/shell/GHTorrent.sh target/classes
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .

run:
	${SPARK_BIN_PATH}spark-submit \
	 	--master local --driver-memory 6g --executor-memory 6G \
	 	--jars "${FULL_RUNTIME_JARS}" \
    	--class org.reactorlabs.jshealth.Main "${JAR_NAME}"
ss:
	${SPARK_BIN_PATH}spark-shell --driver-memory 7G --executor-memory 7G --executor-cores 3 \
	--jars=${FULL_RUNTIME_JARS} \
	--conf spark.scheduler.mode=FAIR \
	--conf spark.checkpoint.compress=true \
	--conf spark.ui.showConsoleProgress=false

aws_ss:
	spark-shell --driver-memory 5G --executor-memory 5G \
	--conf spark.checkpoint.compress=true \
	--conf spark.executor.extraClassPath="/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*" \
	--conf spark.driver.extraClassPath="/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*"


setup: clean mvn_install build

mvn_install:
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*