package cn.xuyinyin.meteor.spi.job;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.Arrays;

/**
 * 极简集成测试 — 验证 Meteor SPI 不影响 Flink 基本功能
 * 数据流: Source → FlatMap → Print（无 shuffle）
 */
public class SimpleTestJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env.fromCollection(
            Arrays.asList("hello", "world", "meteor", "shuffle")
        );

        source.flatMap((FlatMapFunction<String, String>) (value, out) -> out.collect("processed: " + value)).print();

        env.execute("meteor-simple-test");
    }
}
