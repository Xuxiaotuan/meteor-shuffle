package cn.xuyinyin.meteor.spi.job;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Arrays;

/**
 * 并行 map 测试 — 验证多并行度是否有问题
 */
public class ParallelMapJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.fromCollection(
            Arrays.asList("hello", "world", "meteor", "shuffle")
        ).setParallelism(1);

        source.map((MapFunction<String, String>) value -> "mapped: " + value).setParallelism(2).print().setParallelism(1);

        env.execute("meteor-parallel-map-test");
    }
}
