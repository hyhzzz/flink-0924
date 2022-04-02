package com.atguigu.chapter2;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-28 12:01
 * 有界流：所谓的有界流就是有结束边界的流，比如文件
 */
public class Flink01_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1. 创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.1 设置并行度
        env.setParallelism(1);

        //2. 读取文件
        DataStreamSource<String> lineDS = env.readTextFile("input/words.txt");

        //3. 转换数据格式
        //输入和输出
        SingleOutputStreamOperator<Tuple2<String, Long>> result = lineDS.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {

                        for (String word : line.split(" ")) {
                            collector.collect(word);
                        }
                    }
                })
                .map(
                        //Tuple2<String,Long> 第一个是单词 第二个是1
                        new RichMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(String word) throws Exception {
                                return Tuple2.of(word, 1L);
                            }
                        }
                )
                //4. 分组 keyBy:不会更改流中的数据结构，仅仅是对元素进行分组
                .keyBy(new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                        //f0:表示元祖的第一个元素  单词一样的分一组
                        return stringLongTuple2.f0;
                    }
                })
                //5. 求和
                //1：1：对Tuple2中位置1是1的元素做聚合
                .sum(1);

        result.print();

        //启动上下文对象
        env.execute();
    }
}
