package cn.xuyinyin.meteor.common

import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.Route
import io.prometheus.client.{CollectorRegistry, Counter, Gauge, Histogram}
import io.prometheus.client.exporter.common.TextFormat

/**
 * Prometheus 指标 + HTTP metrics 端点
 *
 * 对标 Celeborn 的 Worker/Master metrics
 */
object Metrics {

  // ================================
  // Master 指标
  // ================================
  object MasterMetrics {
    val workersAlive: Gauge = Gauge.build()
      .name("meteor_master_workers_alive")
      .help("Number of alive workers")
      .register()

    val workersTotal: Gauge = Gauge.build()
      .name("meteor_master_workers_total")
      .help("Total registered workers")
      .register()

    val shufflesActive: Gauge = Gauge.build()
      .name("meteor_master_shuffles_active")
      .help("Active shuffle count")
      .register()

    val shuffleRegistrations: Counter = Counter.build()
      .name("meteor_master_shuffle_registrations_total")
      .help("Total shuffle registrations")
      .register()

    val reviveRequests: Counter = Counter.build()
      .name("meteor_master_revive_requests_total")
      .help("Total revive requests")
      .register()
  }

  // ================================
  // Worker 指标
  // ================================
  object WorkerMetrics {
    val pushDataBytes: Counter = Counter.build()
      .name("meteor_worker_push_data_bytes_total")
      .help("Total bytes pushed to worker")
      .register()

    val pushDataRequests: Counter = Counter.build()
      .name("meteor_worker_push_data_requests_total")
      .help("Total push data requests")
      .register()

    val pushDataFailures: Counter = Counter.build()
      .name("meteor_worker_push_data_failures_total")
      .help("Total failed push data requests")
      .register()

    val fetchDataRequests: Counter = Counter.build()
      .name("meteor_worker_fetch_data_requests_total")
      .help("Total fetch data requests")
      .register()

    val fetchDataBytes: Counter = Counter.build()
      .name("meteor_worker_fetch_data_bytes_total")
      .help("Total bytes fetched from worker")
      .register()

    val partitionWriters: Gauge = Gauge.build()
      .name("meteor_worker_partition_writers")
      .help("Current number of active partition writers")
      .register()

    val pushLatency: Histogram = Histogram.build()
      .name("meteor_worker_push_latency_seconds")
      .help("Push data latency")
      .buckets(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
      .register()

    val diskUsage: Gauge = Gauge.build()
      .name("meteor_worker_disk_usage_bytes")
      .help("Disk usage by shuffle data")
      .register()
  }

  // ================================
  // HTTP 端点
  // ================================
  def metricsRoute(registry: CollectorRegistry = CollectorRegistry.defaultRegistry): Route =
    path("metrics") {
      get {
        complete {
          val samples = new java.io.StringWriter()
          TextFormat.write004(samples, registry.metricFamilySamples())
          samples.toString
        }
      }
    }
}
