#!/bin/sh
# Meteor Shuffle Docker Entrypoint
# Supports: master | worker

set -e

ROLE="${1:-master}"
JAVA_OPTS="${JAVA_OPTS:--Xmx2g -Xms1g}"

# Use pod IP for Pekko hostname (K8s downward API)
if [ -n "$PEKKO_HOST" ]; then
    JAVA_OPTS="$JAVA_OPTS -Dpekko.remote.artery.canonical.hostname=$PEKKO_HOST"
fi

echo "=== Meteor Shuffle: starting as $ROLE ==="
echo "PEKKO_HOST: ${PEKKO_HOST:-auto}"
echo "JAVA_OPTS: $JAVA_OPTS"
echo "Classpath: /opt/meteor/lib/*:/opt/meteor/conf"

case "$ROLE" in
    master)
        exec java $JAVA_OPTS \
            -cp /opt/meteor/lib/*:/opt/meteor/conf \
            -Dconfig.file=/opt/meteor/conf/master.conf \
            cn.xuyinyin.meteor.master.MasterApp
        ;;
    worker)
        exec java $JAVA_OPTS \
            -cp /opt/meteor/lib/*:/opt/meteor/conf \
            -Dconfig.file=/opt/meteor/conf/worker.conf \
            cn.xuyinyin.meteor.worker.WorkerApp
        ;;
    *)
        echo "Unknown role: $ROLE (expected master or worker)"
        exit 1
        ;;
esac
