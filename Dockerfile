# Meteor Shuffle — Docker build (pre-built jars)
FROM eclipse-temurin:21-jre

WORKDIR /opt/meteor

COPY meteor-master/target/scala-2.12/meteor-master-assembly-*.jar lib/meteor-master.jar
COPY meteor-worker/target/scala-2.12/meteor-worker-assembly-*.jar lib/meteor-worker.jar
COPY meteor-common/src/main/resources/ /opt/meteor/conf/
COPY meteor-master/src/main/resources/ /opt/meteor/conf/
COPY meteor-worker/src/main/resources/ /opt/meteor/conf/

RUN groupadd -r meteor && useradd -r -g meteor meteor && \
    mkdir -p /data/meteor /tmp/meteor && chown -R meteor:meteor /opt/meteor /data/meteor /tmp/meteor

USER meteor

EXPOSE 2551 8080 9000 2561 8081
