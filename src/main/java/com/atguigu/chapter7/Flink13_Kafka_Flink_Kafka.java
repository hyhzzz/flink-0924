package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

/**
 * @author coderhyh
 * @create 2022-04-01 17:53
 */
class Flink13_Kafka_Flink_Kafka {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint 每隔两秒做一次
        env.enableCheckpointing(2000);

        //设置状态后端
        env.setStateBackend(new HashMapStateBackend()).getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");

        //设置checkpoint一致性级别
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);


        //设置同时并发的Checkpoint的数量，有可能第一个ck没完成，第二个ck开始了，在流里面有可能同时存在多个ck
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //在两个ck之间他的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);


        //设置ck的超时时间，
        env.getCheckpointConfig().setCheckpointTimeout(10 * 1000);


        //ck程序被取消了，ck数据还要不要被保存
        //1.12 旧写法
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //1.13 新写法
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);


        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group_Id", "Flink13_Kafka_Flink_Kafka");
        props.put("auto.reset.offset", "latest");//如果没有上次的消费记录就从最新的位置消费，如果有记录就从上次的位置开始消费
        props.put("isolation.level", "read_committed");//读取已提交数据

        Properties sinkProps = new Properties();
        sinkProps.put("bootstrap.servers", "hadoop102:9092");
        //事务最大超时时间，对生产者来说不允许超过15分钟
        sinkProps.put("transaction.timeout.ms", 15 * 60 * 1000);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env.addSource(new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), props))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        for (String word : s.split(" ")) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);
        stream
                .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>("default", new KafkaSerializationSchema<Tuple2<String, Long>>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> stringLongTuple2, @Nullable Long aLong) {
                        String msg = stringLongTuple2.f0 + "_" + stringLongTuple2.f1;
                        return new ProducerRecord<>("s2", msg.getBytes(StandardCharsets.UTF_8));
                    }
                }, sinkProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                ));

        stream.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
                if (value.f0.contains("x")) {
                    throw new RuntimeException("抛出异常");
                }
            }
        });


        //启动执行环境
        env.execute();
    }
}
