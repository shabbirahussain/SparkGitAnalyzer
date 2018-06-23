RUNTIME_JARS=commons-csv-1.5.jar

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------

STARTUP_FILE=src/main/scala/org/reactorlabs/jshealth/analysis/queries/__init__.scala

BUNDLE_DIR=target/bundle/
JAR_NAME=${BUNDLE_DIR}task.jar

LIB_PATH=target/dependency/
RUNTIME_LIB_PATH=${BUNDLE_DIR}dependency/

CLASSES_PATH=target/classes/
EXTRA_RESOURCES_PATH=${BUNDLE_DIR}resources/

COMMA=,
FULL_RUNTIME_JARS=${LIB_PATH}/$(subst ${COMMA},${COMMA}${LIB_PATH}/,${RUNTIME_JARS}),${JAR_NAME}

build: clean install_deps copy_resources
	mkdir -p "target/artifacts"
	mkdir -p "target/classes/main/resources/"
	scalac -cp "./${LIB_PATH}*" \
		-d target/classes \
		src/main/scala/org/reactorlabs/jshealth/**/*.scala \
		src/main/scala/org/reactorlabs/jshealth/*.scala
	cp -r src/main/resources/* target/classes
	jar cfm ${JAR_NAME} \
		src/main/scala/META-INF/MANIFEST.MF \
		-C target/classes/ .

copy_resources:
	mkdir -p "target/bundle/dependency"
	rm -rf "${CLASSES_PATH}"
	mkdir -p "${CLASSES_PATH}"
	cp ${LIB_PATH}${RUNTIME_JARS} "target/bundle/dependency/"
	cp -r src/main/resources/ ${EXTRA_RESOURCES_PATH}

run: ss

ss:
	spark-shell -i ${STARTUP_FILE} \
		--driver-memory 300G --executor-memory 300G \
    	--master local[*] \
		--files "${EXTRA_RESOURCES_PATH}conf/config-defaults.properties" \
		--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration='file:${PWD}/${EXTRA_RESOURCES_PATH}conf/log4j.properties'" \
		--jars=${FULL_RUNTIME_JARS} \
		--conf spark.scheduler.mode=FAIR \
		--conf spark.checkpoint.compress=true \
		--conf spark.ui.showConsoleProgress=true

install_deps:
	mvn install dependency:copy-dependencies

clean:
	-rm -rf target/*

