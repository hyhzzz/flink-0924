package com.atguigu.chapter5.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-29 0:46
 */
public class Flink01_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 34, 5, 6);

        //<Integer, Object> 输入和输出
        stream.map(new RichMapFunction<Integer, Integer>() {
            @Override
            public void open(Configuration parameters) throws Exception {

                //程序初始化成功后会自动调用这个方法
                //每个并行度执行一次
                System.out.println("open...");

            }

            //integer进来的元素
            @Override
            public Integer map(Integer integer) throws Exception {
                return integer * integer;
            }

            @Override
            public void close() throws Exception {
                //应该关闭的时候执行，执行次数和open一样
                System.out.println("close...");
            }
        }).print();

        env.execute();
    }
}
