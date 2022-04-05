package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-04 10:18
 */
class Flink11_Time_Processiong_1 {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table sensor(" +
                "id string," +
                "ts bigint," +
                "vc int," +
                "pt_time as Proctime())" +
                " with("
                + "'connector' = 'filesystem',"
                + "'path' = 'input/sensor.txt',"
                + "'format' = 'csv'"
                + ")");

        tableEnv.sqlQuery("select * from sensor").execute().print();


    }
}
