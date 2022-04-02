package com.atguigu.chapter5.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author CoderHyh
 * @create 2022-03-29 0:46
 */
public class Flink04_TransForm_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> stream = env.fromElements(1, 2, 34, 4, 5, 6);

        //<Integer, Object>流中元素是什么  key的类型   一般都是用字符串做为key的类型
        stream.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                return value % 2 == 0 ? "偶数" : "奇数";
            }
        }).print();

        env.execute();
    }
}
