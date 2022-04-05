package com.atguigu.chapter11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author coderhyh
 * @create 2022-04-05 19:06
 */
class Flink20_Function_AggTable {
    public static void main(String[] args) {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));

        Table table = tEnv.fromDataStream(waterSensorStream);
        // 1. 内联使用
        //table
        //        .groupBy($("id"))
        //        .flatAggregate(call(Top2.class, $("vc")).as("v", "rank"))
        //        .select($("id"), $("v"), $("rank"))
        //        .execute()
        //        .print();


        // 2. 注册后使用
        tEnv.createTemporaryFunction("top2", Top2.class);
        table
                .groupBy($("id"))
                .flatAggregate(call("top2", $("vc")).as("v", "rank"))
                .select($("id"), $("v"), $("rank"))
                .execute()
                .print();
    }

    // 累加器
    public static class Top2Acc {
        public Integer first = Integer.MIN_VALUE; // top 1
        public Integer second = Integer.MIN_VALUE; // top 2
    }

    // Tuple2<Integer, Integer> 值和排序
    public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Acc> {

        //初始化累加器
        @Override
        public Top2Acc createAccumulator() {
            return new Top2Acc();
        }

        //实现数据累加：计算出来top2的水位，存储到累加器中
        public void accumulate(Top2Acc acc, Integer vc) {
            if (vc > acc.first) {
                acc.second = acc.first;
                acc.first = vc;
            } else if (vc > acc.second) {
                acc.second = vc;
            }
        }

        public void emitValue(Top2Acc acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }

            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }

        }
    }
}
