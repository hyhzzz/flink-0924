package com.atguigu.chapter5.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author CoderHyh
 * @create 2022-03-29 0:46
 */
public class Flink07_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");

        //同床异梦
        intStream.connect(stringStream).map(
                new CoMapFunction<Integer, String, String>() {
                    //处理第一个流的元素
                    @Override
                    public String map1(Integer integer) throws Exception {
                        return integer + "<";
                    }

                    //处理第二个流的元素
                    @Override
                    public String map2(String s) throws Exception {
                        return s + ">";
                    }
                }
        ).print();


        env.execute();
    }
}
