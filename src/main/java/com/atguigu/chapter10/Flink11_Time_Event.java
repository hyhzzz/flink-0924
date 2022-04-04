package com.atguigu.chapter10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author coderhyh
 * @create 2022-04-04 10:18
 */
class Flink11_Time_Event {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> stream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                                new WaterSensor("sensor_1", 2000L, 20),
                                new WaterSensor("sensor_2", 3000L, 30),
                                new WaterSensor("sensor_1", 4000L, 40),
                                new WaterSensor("sensor_1", 5000L, 50),
                                new WaterSensor("sensor_2", 6000L, 60))
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>)
                                        (waterSensor, l) -> waterSensor.getTs()));
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //在转成表的时候添加一个新的字段作为，事件时间
        //Table table = tableEnv.fromDataStream(stream,
        //        $("id"), $("ts"), $("vc"), $("et").rowtime());

        Table table = tableEnv.fromDataStream(stream,
                $("id"), $("ts").rowtime(), $("vc"));
        table.execute().print();


    }
}
