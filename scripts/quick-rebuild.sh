#!/bin/bash
set -euo pipefail
BASE="$(cd "$(dirname "$0")/.." && pwd)"
FLINK_HOME="$HOME/software/flink/flink-1.19.3"
JAR="$BASE/meteor-flink-spi/target/scala-2.13/meteor-flink-spi_2.13-0.1.0-SNAPSHOT.jar"
JOB_CLASS="${1:-cn.xuyinyin.meteor.spi.job.ParallelMapJob}"
TIMEOUT="${2:-45}"
JAVA11_HOME=$(/usr/libexec/java_home -v 11)
export JAVA_HOME="$JAVA11_HOME"
SBT_CMD=(sbt -java-home "$JAVA11_HOME")

echo "=== 1. Compile ==="
echo "JAVA_HOME=$JAVA_HOME"
"$JAVA_HOME/bin/java" -version
cd "$BASE"
COMPILE_LOG=$(mktemp)
"${SBT_CMD[@]}" "meteorFlinkSpi/package" >"$COMPILE_LOG" 2>&1
tail -20 "$COMPILE_LOG"
rm -f "$COMPILE_LOG"

echo "=== 2. Kill Flink ==="
pkill -9 -f "org.apache.flink" 2>/dev/null || true
sleep 3

echo "=== 3. Deploy jars ==="
for mod in meteor-flink-spi meteor-common meteor-client; do
  cp "$BASE/$mod/target/scala-2.13/"meteor-*.jar "$FLINK_HOME/lib/" 2>/dev/null
  cp "$BASE/$mod/target/scala-2.13/"meteor-*.jar "$FLINK_HOME/plugins/meteor-shuffle/" 2>/dev/null
done
echo "OK"

echo "=== 4. Start cluster ==="
"$FLINK_HOME/bin/start-cluster.sh" 2>&1
sleep 8

echo "=== 5. Submit $JOB_CLASS ==="
timeout "$TIMEOUT" "$FLINK_HOME/bin/flink" run -c "$JOB_CLASS" "$JAR" 2>&1

echo ""
echo "=== TM Log (Meteor) ==="
LATEST_TM=$(ls -t "$FLINK_HOME/log/"*taskexecutor-*.log 2>/dev/null | head -1)
grep "\[Meteor" "$LATEST_TM" 2>/dev/null | tail -20
echo ""
echo "=== TM Log (ERROR) ==="
grep "ERROR\|Exception\|Caused by:" "$LATEST_TM" 2>/dev/null | tail -10
