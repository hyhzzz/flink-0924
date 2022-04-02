package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-31 19:05
 */
public class Flink09_KeyedState_Value {
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
                .keyBy(WaterSensor::getId)
                .flatMap(new RichFlatMapFunction<WaterSensor, String>() {


                    private ValueState<Integer> lastVcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //获取单值状态
                        lastVcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class) {
                        });

                    }

                    @Override
                    public void flatMap(WaterSensor value,
                                        Collector<String> collector) throws Exception {

                        //如果是第一条数据，则把水位存到状态中，如果不是第一条，则把状态中的水位值取出
                        //不是第一条
                        if (lastVcState.value() != null) {

                            Integer lastVc = lastVcState.value();
                            if (lastVc > 10 && value.getVc() > 10) {
                                collector.collect(value.getId() + "连续两次水位超过10，发出橙色预警");
                            }
                        }
                        //第一条数据
                        lastVcState.update(value.getVc());
                    }
                }).print();

        //启动执行环境
        env.execute();
    }
}
