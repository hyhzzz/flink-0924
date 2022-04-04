package com.atguigu.chapter10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneOffset;

/**
 * @author coderhyh
 * @create 2022-04-04 10:18
 */
class Flink12_Time_Event_1 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().setLocalTimeZone(ZoneOffset.ofHours(0));

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

        tableEnv.sqlQuery("select * from sensor").execute().print();


    }
}
