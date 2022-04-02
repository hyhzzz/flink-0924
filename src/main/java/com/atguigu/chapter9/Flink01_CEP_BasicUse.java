package com.atguigu.chapter9;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-02 12:31
 */
class Flink01_CEP_BasicUse {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        //启动执行环境
        env.execute();
    }
}
