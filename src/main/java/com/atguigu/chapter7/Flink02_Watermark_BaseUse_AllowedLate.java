package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-31 12:28
 */
public class Flink02_Watermark_BaseUse_AllowedLate {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)) //设置乱序程度
                                //WatermarkStrategy.<WaterSensor>forMonotonousTimestamps() //设置有序程度
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    //返回事件时间
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long l) {
                                        return waterSensor.getTs();
                                    }
                                })
                        //.withIdleness(Duration.ofSeconds(10)) //防止数据倾斜带来的水印不更新
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .allowedLateness(Time.seconds(2))//设置允许迟到
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context,
                                        Iterable<WaterSensor> elements,
                                        Collector<String> collector) throws Exception {


                        List<WaterSensor> list = AtguiguUtil.toList(elements);
                        collector.collect(key + " " + context.window() + "" + list);

                    }
                }).print();


        //启动执行环境
        env.execute();
    }
}
