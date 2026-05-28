val scala213           = "2.12.20"
val projectVersion     = "0.1.0-SNAPSHOT"
val projectOrg         = "cn.xuyinyin"
val pekkoVersion       = "1.1.3"
val pekkoHttpVersion   = "1.0.1"
val logbackVersion     = "1.5.13"
val scalaLoggingVer    = "3.9.5"
val scalatestVersion   = "3.2.19"
val sprayJsonVersion   = "1.3.6"
val jacksonVersion     = "2.17.2"
val prometheusVersion  = "0.16.0"
val levelDbVersion     = "1.8"
val levelDbApiVersion  = "0.12"

ThisBuild / scalaVersion := scala213
ThisBuild / version      := projectVersion
ThisBuild / organization := projectOrg
ThisBuild / fork         := true

lazy val commonSettings = Seq(
  Compile / scalacOptions ++= Seq(
    "-deprecation", "-feature", "-unchecked", "-Xlint"
  ),
  Compile / javacOptions ++= Seq(
    "-source", "11", "-target", "11"
  ),
  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingVer,
    "ch.qos.logback"              % "logback-classic"  % logbackVersion,
    "org.scalatest"              %% "scalatest"        % scalatestVersion % Test
  )
)

// ================================
// meteor-common: 协议 + 工具
// ================================
lazy val meteorCommon = (project in file("meteor-common"))
  .settings(commonSettings)
  .settings(
    name := "meteor-common",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed"           % pekkoVersion,
      "org.apache.pekko" %% "pekko-serialization-jackson"  % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"            % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-stream"              % pekkoVersion,
      "io.spray"         %% "spray-json"                   % sprayJsonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "io.prometheus"    % "simpleclient"                  % prometheusVersion,
      "io.prometheus"    % "simpleclient_common"          % prometheusVersion,
      "io.prometheus"    % "simpleclient_hotspot"          % prometheusVersion,
      "org.lz4"          % "lz4-java"                      % "1.8.0",
      "org.xerial.snappy" % "snappy-java"                   % "1.1.10.5",
      "com.github.luben"  % "zstd-jni"                      % "1.5.6-3"
    )
  )

// ================================
// meteor-master: 控制面（3 节点 HA）
// ================================
lazy val meteorMaster = (project in file("meteor-master"))
  .dependsOn(meteorCommon)
  .dependsOn(meteorWorker % "test")
  .settings(commonSettings)
  .settings(
    name := "meteor-master",
    Compile / mainClass := Some("cn.xuyinyin.meteor.master.MasterApp"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-cluster-typed"          % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-sharding-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-tools"          % pekkoVersion,
      "org.apache.pekko" %% "pekko-persistence-typed"      % pekkoVersion,
      "org.apache.pekko" %% "pekko-persistence-query"      % pekkoVersion,
      "org.fusesource.leveldbjni" % "leveldbjni-all"       % levelDbVersion,
      "org.iq80.leveldb"          % "leveldb"              % levelDbApiVersion,
      "org.apache.pekko" %% "pekko-http"                   % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json"        % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-actor-testkit-typed"    % pekkoVersion    % Test,
      "org.apache.pekko" %% "pekko-persistence-testkit"    % pekkoVersion    % Test,
      "org.apache.pekko" %% "pekko-multi-node-testkit"     % pekkoVersion    % Test
    )
  )

// ================================
// meteor-worker: 数据面
// ================================
lazy val meteorWorker = (project in file("meteor-worker"))
  .dependsOn(meteorCommon, meteorTransport)
  .settings(commonSettings)
  .settings(
    name := "meteor-worker",
    Compile / mainClass := Some("cn.xuyinyin.meteor.worker.WorkerApp"),
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-cluster-typed"       % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"              % pekkoVersion,
      "org.apache.pekko" %% "pekko-http"                % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json"     % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-stream-testkit"      % pekkoVersion    % Test
    )
  )

// ================================
// meteor-client: 嵌入 JM/TM 的 Shuffle Client
// ================================
lazy val meteorClient = (project in file("meteor-client"))
  .dependsOn(meteorCommon)
  .settings(commonSettings)
  .settings(
    name := "meteor-client",
    libraryDependencies ++= Seq(
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream"      % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream-testkit" % pekkoVersion % Test
    )
  )

// ================================
// meteor-flink-spi: Flink ShuffleServiceFactory SPI 适配
// ================================
val nettyVersion       = "4.1.110.Final"

// ================================
// meteor-transport: Netty 直连传输层（数据面）
// ================================
lazy val meteorTransport = (project in file("meteor-transport"))
  .dependsOn(meteorCommon)
  .settings(commonSettings)
  .settings(
    name := "meteor-transport",
    libraryDependencies ++= Seq(
      "io.netty" % "netty-all" % nettyVersion
    )
  )

val flinkVersion = "1.19.3"

lazy val meteorFlinkSpi = (project in file("meteor-flink-spi"))
  .dependsOn(meteorClient)
  .settings(commonSettings)
  .settings(
    name := "meteor-flink-spi",
    assembly / assemblyMergeStrategy := {
      case PathList("module-info.class") => MergeStrategy.discard
      case PathList("META-INF", "versions", "9", "module-info.class") => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    },
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-runtime" % flinkVersion % Provided,
      "org.apache.flink" % "flink-streaming-java" % flinkVersion % Provided,
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion
    )
  )

// ================================
// 根项目
// ================================
lazy val root = (project in file("."))
  .settings(commonSettings)
  .aggregate(meteorCommon, meteorMaster, meteorWorker, meteorClient, meteorFlinkSpi, meteorTransport)
  .settings(
    name := "meteor-shuffle",
    publish := {},
    publishLocal := {},
    addCommandAlias("runMaster",  "meteorMaster/run"),
    addCommandAlias("runWorker",  "meteorWorker/run"),
    addCommandAlias("testAll",    "test"),
    addCommandAlias("cleanAll",   "clean; meteorCommon/clean; meteorMaster/clean; meteorWorker/clean; meteorClient/clean")
  )
