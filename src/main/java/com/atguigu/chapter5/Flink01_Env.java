package com.atguigu.chapter5;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-28 23:53
 */
public class Flink01_Env {
    public static void main(String[] args) {
        // 批处理环境
        ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        // 流式数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
