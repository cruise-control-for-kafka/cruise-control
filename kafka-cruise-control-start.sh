#!/bin/bash
# Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] [-jars jar_path] config_path [port]"
  exit 1
fi

# CYGINW == 1 if Cygwin is detected, else 0.
if [[ $(uname -a) =~ "CYGWIN" ]]; then
  CYGWIN=1
else
  CYGWIN=0
fi

copyJars() {
  if [ -z $1 ]; then
    return 0
  fi
  files=$1
  jars=(${files//,/ })
  for usrJar in ${jars[@]};
  do
    cp $usrJar "$base_dir"/cruise-control/build/dependant-libs/
  done
}

base_dir=$(dirname $0)

if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.13.6
fi

if [ -z "$SCALA_BINARY_VERSION" ]; then
  SCALA_BINARY_VERSION=$(echo $SCALA_VERSION | cut -f 1-2 -d '.')
fi

# run ./gradlew copyDependantLibs to get all dependant jars in a local dir
shopt -s nullglob
for dir in "$base_dir"/cruise-control/build/dependant-libs;
do
  if [ -z "$CLASSPATH" ] ; then
    CLASSPATH="$dir/*"
  else
    CLASSPATH="$CLASSPATH:$dir/*"
  fi
done

if [ -z "$CLASSPATH" ]; then
  CLASSPATH="$base_dir/cruise-control/build/libs/*"
else
  CLASSPATH="$CLASSPATH:$base_dir/cruise-control/build/libs/*"
fi

if [ -z "$CLASSPATH" ]; then
  CLASSPATH="$base_dir/cruise-control-metrics-reporter/build/libs/*"
else
  CLASSPATH="$CLASSPATH:$base_dir/cruise-control-metrics-reporter/build/libs/*"
fi
shopt -u nullglob

# Backward compatibility for deprecated KAFKA_* env vars (remove after migration)
: "${JMX_OPTS:=${KAFKA_JMX_OPTS:-}}"
: "${LOG4J_OPTS:=${KAFKA_LOG4J_OPTS:-}}"
: "${JVM_OPTS:=${KAFKA_OPTS:-}}"
: "${HEAP_OPTS:=${KAFKA_HEAP_OPTS:-}}"
: "${JVM_PERFORMANCE_OPTS:=${KAFKA_JVM_PERFORMANCE_OPTS:-}}"
: "${GC_LOG_OPTS:=${KAFKA_GC_LOG_OPTS:-}}"
: "${DEBUG:=${KAFKA_DEBUG:-}}"

# JMX settings
if [ -z "$JMX_OPTS" ]; then
  JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  JMX_OPTS="$JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "$LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  LOG4J_DIR="$base_dir/config/log4j.properties"
  # If Cygwin is detected, LOG4J_DIR is converted to Windows format.
  (( CYGWIN )) && LOG4J_DIR=$(cygpath --path --mixed "${LOG4J_DIR}")
  LOG4J_OPTS="-Dlog4j.configurationFile=file:${LOG4J_DIR}"
else
  # create logs directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
  fi
fi

# Generic jvm settings you want to add
if [ -z "$JVM_OPTS" ]; then
  JVM_OPTS=""
fi

# Set Debug options if enabled
if [ "x$DEBUG" != "x" ]; then

    # Use default ports
    DEFAULT_JAVA_DEBUG_PORT="5005"

    if [ -z "$JAVA_DEBUG_PORT" ]; then
        JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
    fi

    # Use the defaults if JAVA_DEBUG_OPTS was not set
    DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT"
    if [ -z "$JAVA_DEBUG_OPTS" ]; then
        JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
    fi

    echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    JVM_OPTS="$JAVA_DEBUG_OPTS $JVM_OPTS"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$HEAP_OPTS" ]; then
  HEAP_OPTS="-Xmx1G"
fi

# JVM performance options
if [ -z "$JVM_PERFORMANCE_OPTS" ]; then
  JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

#Add jaas file to JVM_OPTS if present
if [ -f $base_dir/config/cruise_control_jaas.conf ]
then
  JVM_OPTS="-Djava.security.auth.login.config=$base_dir/config/cruise_control_jaas.conf $JVM_OPTS"
fi

DAEMON_NAME="kafka-cruise-control"
CONSOLE_OUTPUT_FILE="${LOG_DIR}"/"${DAEMON_NAME}".out
while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE="${LOG_DIR}"/"${DAEMON_NAME}".out
      shift 2
      ;;
    -loggc)
      if [ -z "$GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    -jars)
      JAR_PATHS=$2
      copyJars "$JAR_PATHS"
      shift 2
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
fi

# If Cygwin is detected, classpath is converted to Windows format.
(( CYGWIN )) && CLASSPATH=$(cygpath --path --mixed "${CLASSPATH}")

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS $JMX_OPTS $LOG4J_OPTS -cp $CLASSPATH $JVM_OPTS com.linkedin.kafka.cruisecontrol.KafkaCruiseControlMain "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $GC_LOG_OPTS $JMX_OPTS $LOG4J_OPTS -cp $CLASSPATH $JVM_OPTS com.linkedin.kafka.cruisecontrol.KafkaCruiseControlMain "$@"
fi
