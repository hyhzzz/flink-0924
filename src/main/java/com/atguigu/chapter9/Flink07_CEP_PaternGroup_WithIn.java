package com.atguigu.chapter9;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author coderhyh
 * @create 2022-04-02 12:31
 */
class Flink07_CEP_PaternGroup_WithIn {
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
        GroupPattern<WaterSensor, WaterSensor> pattern = Pattern.begin(Pattern.<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "sensor_1".equals(waterSensor.getId());
                    }
                })
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor waterSensor) throws Exception {
                        return "sensor_2".equals(waterSensor.getId());
                    }
                })
                //.times(2)
                //超时数据
                .within(Time.seconds(2))
        );

        //在流上应用模式
        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);

        //获取匹配到的结果
        //参数就是匹配的时候，超时的数据
//返回值就是要放入侧输出流的数据
        SingleOutputStreamOperator<String> normal = ps.select(
                new OutputTag<WaterSensor>("timeout") {
                }, (PatternTimeoutFunction<WaterSensor, WaterSensor>) (map, l) -> map.get("s1").get(0),
                (PatternSelectFunction<WaterSensor, String>) map -> map.toString());

        normal.print("匹配成功...");
        normal.getSideOutput(new OutputTag<WaterSensor>("timeout") {
        }).print("timeout");


        //启动执行环境
        env.execute();
    }
}
