package cn.xuyinyin.meteor.spi.job

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.api.common.functions.{RichMapFunction, ReduceFunction, RichFlatMapFunction, MapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.util.Collector
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.typeinfo.{TypeInformation, TypeHint, Types}
import scala.collection.JavaConverters._

/**
 * Meteor Shuffle 集成测试 Job (Java DataStream API)。
 *
 * 数据流: Source → FlatMap (放大 10x) → KeyBy (SHUFFLE!) → Reduce → Print
 *
 * 提交方式:
 *   flink run -c cn.xuyinyin.meteor.spi.job.ShuffleIntegrationJob meteor-flink-spi.jar
 */
object ShuffleIntegrationJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // 输入: [(apple,1), (banana,2), ...]
    val items: java.util.List[JTuple2[String, java.lang.Long]] = java.util.Arrays.asList(
      new JTuple2("apple",      java.lang.Long.valueOf(1L)),
      new JTuple2("banana",     java.lang.Long.valueOf(2L)),
      new JTuple2("cherry",     java.lang.Long.valueOf(3L)),
      new JTuple2("date",       java.lang.Long.valueOf(4L)),
      new JTuple2("elderberry", java.lang.Long.valueOf(5L)),
      new JTuple2("fig",        java.lang.Long.valueOf(6L)),
      new JTuple2("grape",      java.lang.Long.valueOf(7L)),
      new JTuple2("honeydew",   java.lang.Long.valueOf(8L))
    )

    // 使用 TypeHint 避免泛型擦除问题
    val tupleType: TypeInformation[JTuple2[String, java.lang.Long]] =
      TypeInformation.of(new TypeHint[JTuple2[String, java.lang.Long]] {})

    val source = env.fromCollection(items).setParallelism(1)
      .returns(tupleType)

    // FlatMap: 每个元素生成 10 个变体 → 80 条记录
    val expanded = source
      .flatMap(new RichFlatMapFunction[JTuple2[String, java.lang.Long], JTuple2[String, java.lang.Long]] {
        override def flatMap(
          value: JTuple2[String, java.lang.Long],
          out: Collector[JTuple2[String, java.lang.Long]]
        ): Unit = {
          // 用 while 替代 for-comprehension，避免 Scala 2.13 运行时依赖
          var i = 1
          while (i <= 10) {
            out.collect(new JTuple2(value.f0 + "-" + i, value.f1))
            i += 1
          }
        }
      })
      .returns(tupleType)
      .setParallelism(2) // 并行 → 触发 shuffle

    // KeyBy + Reduce → SHUFFLE!
    val result = expanded
      // 用匿名类替代 lambda，避免 Scala 2.13 lambda 序列化在 Java 25 上的兼容问题
      .keyBy(new KeySelector[JTuple2[String, java.lang.Long], String] {
        override def getKey(v: JTuple2[String, java.lang.Long]): String = v.f0
      })
      .reduce(new ReduceFunction[JTuple2[String, java.lang.Long]] {
        override def reduce(
          a: JTuple2[String, java.lang.Long],
          b: JTuple2[String, java.lang.Long]
        ): JTuple2[String, java.lang.Long] = {
          // 用 Java 风格的 unbox 加法，避免 Scala 运行时依赖
          new JTuple2(a.f0, java.lang.Long.valueOf(a.f1.longValue() + b.f1.longValue()))
        }
      })
      .setParallelism(2) // 两个 reducer

    // 输出
    result
      .map(new MapFunction[JTuple2[String, java.lang.Long], String] {
        override def map(v: JTuple2[String, java.lang.Long]): String =
          "RESULT: " + v.f0 + " -> " + v.f1
      })
      .print().setParallelism(1)

    env.execute("meteor-shuffle-integration-test")
  }
}
