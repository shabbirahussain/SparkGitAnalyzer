RUNTIME_JARS=commons-csv-1.5.jar

PRL_USER=hshabbir
PRL_MACHINE=${PRL_USER}@prl1c
PRL_DB_NAME=hshabbir_reactorlabs

# ------------------------------------
# Do not edit! Local config variables.
# ------------------------------------

STARTUP_FILE=queries/__init__.scala

BUNDLE_DIR=target/bundle/
JAR_NAME=${BUNDLE_DIR}task.jar

LIB_PATH=target/dependency/
RUNTIME_LIB_PATH=${BUNDLE_DIR}dependency/

CLASSES_PATH=target/classes/
EXTRA_RESOURCES_PATH=${BUNDLE_DIR}resources/

DEPLOYABLE_DIR=target/deployable/
DEPLOYABLE_TAR=target/deployable.tar
DEPLOYABLE_PAYLOAD_DIR=payload/

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

run: prl_deploy prl_tunnel

ss:
	spark-shell -i ${EXTRA_RESOURCES_PATH}${STARTUP_FILE} \
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

create_deployable: build
	rm -rf ${DEPLOYABLE_DIR}
	mkdir -p ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	cp Makefile ${DEPLOYABLE_DIR}
	cp -r ${BUNDLE_DIR} ${DEPLOYABLE_DIR}${BUNDLE_DIR}
	-cp -r ${DEPLOYABLE_PAYLOAD_DIR} ${DEPLOYABLE_DIR}${DEPLOYABLE_PAYLOAD_DIR}
	tar -C ${DEPLOYABLE_DIR} -cvf target/deployable.tar .
	rm -rf ${DEPLOYABLE_DIR}

# =================================================================================
# PRL Specific scripts and configuration
# ---------------------------------------------------------------------------------
prl_deploy: create_deployable
	scp ${DEPLOYABLE_TAR} ${PRL_MACHINE}:/home/${PRL_USER}/SparkGitAnalyzer

prl_tunnel:
	-open http://localhost:9040/jobs/
	-xdg-open http://localhost:9040/jobs/
	ssh -L 9040:localhost:4040 ${PRL_MACHINE} -t '\
		. /home/hshabbir/.bash_profile \
	    echo $$PATH;\
		cd ~/SparkGitAnalyzer;\
		tar -xvf deployable.tar;\
		make ss;\
		bash -l;\
	'
prl_ssh:
	ssh ${PRL_MACHINE}


