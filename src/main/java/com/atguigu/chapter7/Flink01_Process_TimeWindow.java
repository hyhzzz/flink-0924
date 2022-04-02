package com.atguigu.chapter7;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * @author CoderHyh
 * @create 2022-03-30 13:00
 */
public class Flink01_Process_TimeWindow {
    public static void main(String[] args) throws Exception {


        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        env.socketTextStream("hadoop102", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> collector) throws Exception {

                        for (String word : value.split(",")) {
                            collector.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(5))) //滚动窗口
                //.timeWindow(Time.seconds(5))// 旧的写法 滚动窗口 需要设置时间语义

                //开一个五秒窗口，每一个窗口两秒
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(2)))// 滑动窗口

                //三秒没有来数据 这个窗口就会关闭
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))//会话窗口

                //.sum(1)
                // 流的输入   流的输出  key的类型  窗口的类型
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<Tuple2<String, Long>, String,
                                                String, TimeWindow>.Context context,
                                        Iterable<Tuple2<String, Long>> elements, //存储了窗口内所有的元素
                                        Collector<String> collector) throws Exception {

                        //窗口开始时间
                        Date start = new Date(context.window().getStart());
                        //窗口结束时间
                        Date end = new Date(context.window().getEnd());

                        int count = 0;

                        for (Tuple2<String, Long> element : elements) {
                            count++;
                        }
                        collector.collect("key=" + key + "窗口开始时间" + start + "窗口结束" + end + "个数：" + count);

                    }
                })
                .print();

        //启动执行环境
        env.execute();
    }
}
