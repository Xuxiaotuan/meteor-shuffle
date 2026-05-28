package cn.xuyinyin.meteor.spi.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

/**
 * Java shuffle 集成测试 — 纯 Java，无 Scala 依赖
 * 数据流: Source → FlatMap → KeyBy (SHUFFLE!) → Reduce → Print
 */
public class JavaShuffleJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Long>> items = new ArrayList<>();
        items.add(new Tuple2<>("apple", 1L));
        items.add(new Tuple2<>("banana", 2L));
        items.add(new Tuple2<>("cherry", 3L));

        DataStream<Tuple2<String, Long>> source = env.fromCollection(items)
            .setParallelism(1);

        // FlatMap: 每个元素展开 3 个
        DataStream<Tuple2<String, Long>> expanded = source
            .flatMap((FlatMapFunction<Tuple2<String, Long>, Tuple2<String, Long>>) (value, out) -> {
                for (int i = 1; i <= 3; i++) {
                    out.collect(new Tuple2<>(value.f0 + "-" + i, value.f1));
                }
            })
            .setParallelism(2);

        // KeyBy + Reduce → SHUFFLE!
        DataStream<Tuple2<String, Long>> result = expanded
            .keyBy((KeySelector<Tuple2<String, Long>, String>) v -> v.f0)
            .reduce((ReduceFunction<Tuple2<String, Long>>) (a, b) -> new Tuple2<>(a.f0, a.f1 + b.f1))
            .setParallelism(2);

        result.print().setParallelism(1);

        env.execute("meteor-java-shuffle-test");
    }
}
