package com.atguigu.chapter9;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author coderhyh
 * @create 2022-04-02 12:31
 */
class Flink05_CEP_Group {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("input/sensor.txt")
                .map((MapFunction<String, WaterSensor>) value -> {
                    String[] split = value.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]) * 1000,
                            Integer.parseInt(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTs()));

        //定义模式
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                //组合模式
                //.next("s2") //严格紧邻
                //.followedBy("s2")//非严格紧邻
                .followedByAny("s2") //非确定的松散连续
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "sensor_2".equals(waterSensor.getId());
                    }
                });

        //在流上应用模式
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);

        //获取匹配到的结果
        ps.select(new PatternSelectFunction<WaterSensor, String>() {
            //每匹配成功一次，这个方法就执行一次
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                return map.toString();
            }
        }).print();

        //启动执行环境
        env.execute();
    }
}
