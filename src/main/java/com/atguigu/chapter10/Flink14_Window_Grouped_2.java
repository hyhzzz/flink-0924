package com.atguigu.chapter10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-04 10:18
 */
class Flink14_Window_Grouped_2 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = env
                .fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs())
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 作为事件时间的字段必须是 timestamp 类型, 所以根据 long 类型的 ts 计算出来一个 t
        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int, " +
                "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for t as t - interval '5' second)" +
                "with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")");


  /*      tableEnv
                .sqlQuery(
                        "SELECT id, " +
                                "  TUMBLE_START(t, INTERVAL '1' minute) as wStart,  " +
                                "  TUMBLE_END(t, INTERVAL '1' minute) as wEnd,  " +
                                "  SUM(vc) sum_vc " +
                                "FROM sensor " +
                                "GROUP BY TUMBLE(t, INTERVAL '1' minute), id"
                )
                .execute()
                .print();*/

        tableEnv
                .sqlQuery(
                        "SELECT id, " +
                                "  hop_start(t, INTERVAL '1' minute, INTERVAL '1' hour) as wStart,  " +
                                "  hop_end(t, INTERVAL '1' minute, INTERVAL '1' hour) as wEnd,  " +
                                "  SUM(vc) sum_vc " +
                                "FROM sensor " +
                                "GROUP BY hop(t, INTERVAL '1' minute, INTERVAL '1' hour), id"
                )
                .execute()
                .print();


    }
}
