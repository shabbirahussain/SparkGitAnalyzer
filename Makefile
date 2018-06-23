RUNTIME_JARS=commons-csv-1.5.jar

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------
JAR_NAME=target/artifacts/task.jar
LIB_PATH=target/dependency
AWS_DIR=target/aws
STARTUP_FILE=src/main/resources/Startup.scala

COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}/$(subst ${COMMA},${COMMA}${LIB_PATH}/,${RUNTIME_JARS}),${JAR_NAME}

all: clean_build ss

clean_build: clean install_deps build

build:
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	scalac -cp "./${LIB_PATH}/*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	cp -r src/main/resources/* target/classes
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .

ss:
	spark-shell -i ${STARTUP_FILE} \
		--driver-memory 300G --executor-memory 300G \
    	--master local[*] \
		--jars=${FULL_RUNTIME_JARS} \
		--conf spark.scheduler.mode=FAIR \
		--conf spark.checkpoint.compress=true \
		--conf spark.ui.showConsoleProgress=false

install_deps:
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*

