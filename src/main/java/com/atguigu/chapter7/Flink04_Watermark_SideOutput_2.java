package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author CoderHyh
 * @create 2022-03-31 12:28
 * 分流
 */
public class Flink04_Watermark_SideOutput_2 {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> main = env.socketTextStream("hadoop102", 9999)
                .map(value -> {
                    String[] data = value.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor ws,
                                               ProcessFunction<WaterSensor, WaterSensor>.Context context,
                                               Collector<WaterSensor> collector) throws Exception {

                        //sensor1放入主流，sensor2放入侧输出流  其他放入一个侧输出流
                        if ("sensor_1".equals(ws.getId())) {
                            collector.collect(ws);
                        } else if ("sensor_2".equals(ws.getId())) {
                            //output():第一个表示一个侧输出流，第二个是数据，意思就是把这个数据放到这个侧输出流中去
                            context.output(new OutputTag<WaterSensor>("s2") {
                            }, ws);
                        } else {
                            context.output(new OutputTag<WaterSensor>("other") {
                            }, ws);
                        }

                    }
                });

        main.print("主流");
        main.getSideOutput(new OutputTag<WaterSensor>("s2") {
        }).print("s2");
        main.getSideOutput(new OutputTag<WaterSensor>("other") {
        }).print("other");


        //启动执行环境
        env.execute();
    }
}
