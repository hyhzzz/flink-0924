package com.atguigu.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-05 1:49
 */
class Flink16_Window_Over_2 {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        //
        //tableEnv
        //        .sqlQuery(
        //                "select " +
        //                        "id," +
        //                        "vc," +
        //                        "ts," +
        //                        "sum(vc) over(partition by id order by t rows between 1 PRECEDING and current row)" +
        //                        "from sensor"
        //        )
        //        .execute()
        //        .print();


        tableEnv
                .sqlQuery(
                        "select " +
                                "id," +
                                "vc," +
                                "ts," +
                                "count(vc) over w, " +
                                "sum(vc) over w " +
                                "from sensor " +
                                "window w as (partition by id order by t rows between 1 PRECEDING and current row)"
                )
                .execute()
                .print();


        //启动执行环境
        env.execute();

    }
}
