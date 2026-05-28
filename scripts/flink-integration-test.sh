#!/bin/bash
# meteor-shuffle Flink 集成测试脚本
# 自动启动 meteor Master + Worker，配置 Flink，提交 shuffle 测试 job
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
FLINK_HOME="${FLINK_HOME:-$HOME/software/flink/flink-1.19.3}"
METEOR_DIR="$SCRIPT_DIR"
PLUGIN_DIR="$FLINK_HOME/plugins/meteor-shuffle"

echo "=========================================="
echo " Meteor Shuffle + Flink 集成测试"
echo "=========================================="
echo "Flink: $FLINK_HOME"
echo "Meteor: $METEOR_DIR"
echo ""

# Step 1: Build meteor jars
echo "[1/5] Building meteor-shuffle..."
cd "$METEOR_DIR"
sbt package 2>&1 | tail -3

# Step 2: Deploy plugin to Flink
echo "[2/5] Deploying meteor plugin to Flink..."
rm -rf "$PLUGIN_DIR"
mkdir -p "$PLUGIN_DIR"

# Copy meteor jars
for mod in meteor-common meteor-client meteor-flink-spi meteor-transport; do
  jar="$METEOR_DIR/$mod/target/scala-2.13/${mod}_2.13-0.1.0-SNAPSHOT.jar"
  if [ -f "$jar" ]; then
    cp "$jar" "$PLUGIN_DIR/"
    echo "  Copied $mod"
  fi
done

# Copy Pekko + Netty + compression jars from coursier cache
CP="$HOME/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2"
DEPS=(
  "org/apache/pekko/pekko-actor_2.13/1.1.3/pekko-actor_2.13-1.1.3.jar"
  "org/apache/pekko/pekko-actor-typed_2.13/1.1.3/pekko-actor-typed_2.13-1.1.3.jar"
  "org/apache/pekko/pekko-stream_2.13/1.1.3/pekko-stream_2.13-1.1.3.jar"
  "org/apache/pekko/pekko-slf4j_2.13/1.1.3/pekko-slf4j_2.13-1.1.3.jar"
  "org/apache/pekko/pekko-protobuf-v3_2.13/1.1.3/pekko-protobuf-v3_2.13-1.1.3.jar"
  "org/apache/pekko/pekko-serialization-jackson_2.13/1.1.3/pekko-serialization-jackson_2.13-1.1.3.jar"
  "io/netty/netty-all/4.1.110.Final/netty-all-4.1.110.Final.jar"
  "org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar"
  "org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar"
  "com/github/luben/zstd-jni/1.5.6-3/zstd-jni-1.5.6-3.jar"
  "com/typesafe/config/1.4.3/config-1.4.3.jar"
  "org/reactivestreams/reactive-streams/1.0.4/reactive-streams-1.0.4.jar"
  "com/fasterxml/jackson/core/jackson-core/2.17.3/jackson-core-2.17.3.jar"
  "com/fasterxml/jackson/core/jackson-databind/2.17.3/jackson-databind-2.17.3.jar"
  "com/fasterxml/jackson/core/jackson-annotations/2.17.3/jackson-annotations-2.17.3.jar"
  "com/fasterxml/jackson/module/jackson-module-scala_2.13/2.17.3/jackson-module-scala_2.13-2.17.3.jar"
  "com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.17.3/jackson-datatype-jdk8-2.17.3.jar"
  "com/fasterxml/jackson/datatype/jackson-datatype-jsr310/2.17.3/jackson-datatype-jsr310-2.17.3.jar"
  "com/fasterxml/jackson/dataformat/jackson-dataformat-cbor/2.17.3/jackson-dataformat-cbor-2.17.3.jar"
  "com/fasterxml/jackson/module/jackson-module-parameter-names/2.17.3/jackson-module-parameter-names-2.17.3.jar"
  "org/scala-lang/scala-library/2.13.12/scala-library-2.13.12.jar"
  "org/scala-lang/scala-reflect/2.13.12/scala-reflect-2.13.12.jar"
  "com/typesafe/scala-logging/scala-logging_2.13/3.9.5/scala-logging_2.13-3.9.5.jar"
  "io/prometheus/simpleclient/0.16.0/simpleclient-0.16.0.jar"
  "io/prometheus/simpleclient_common/0.16.0/simpleclient_common-0.16.0.jar"
)
for dep in "${DEPS[@]}"; do
  if [ -f "$CP/$dep" ]; then
    cp "$CP/$dep" "$PLUGIN_DIR/"
  else
    echo "  WARNING: $dep not found in cache"
  fi
done

echo "  Plugin dir: $PLUGIN_DIR ($(ls "$PLUGIN_DIR" | wc -l) jars)"

# Step 3: Configure Flink
echo "[3/5] Configuring Flink..."
CONF="$FLINK_HOME/conf/flink-conf.yaml"

# Backup original config
if [ ! -f "$CONF.bak" ]; then
  cp "$CONF" "$CONF.bak"
fi

# Set parallelism low for local test
cat >> "$CONF" << 'EOF'

# Meteor Shuffle Configuration
network.shuffle-service-factory.class: cn.xuyinyin.meteor.spi.MeteorShuffleServiceFactory
meteor.master.host: 127.0.0.1
meteor.master.port: 7337
meteor.client.push.replicate: false
taskmanager.memory.process.size: 1024m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 2
EOF

echo "  Config updated in $CONF"

# Step 4: Start Flink cluster
echo "[4/5] Starting Flink cluster..."
"$FLINK_HOME/bin/stop-cluster.sh" 2>/dev/null || true
sleep 2
"$FLINK_HOME/bin/start-cluster.sh"
sleep 5
echo "  Flink started"

# Step 5: Submit test job
echo "[5/5] Submitting shuffle test job..."
JOB_JAR="$METEOR_DIR/meteor-flink-spi/target/scala-2.13/meteor-flink-spi_2.13-0.1.0-SNAPSHOT.jar"

# Need flink-streaming-java on classpath — use Flink's own
FLINK_LIB="$FLINK_HOME/lib"

"$FLINK_HOME/bin/flink" run \
  -c cn.xuyinyin.meteor.spi.job.ShuffleIntegrationJob \
  "$JOB_JAR"

echo ""
echo "=========================================="
echo " Done! Check Flink Web UI: http://localhost:8081"
echo " To restore config: cp $CONF.bak $CONF"
echo " To stop: $FLINK_HOME/bin/stop-cluster.sh"
echo "=========================================="
