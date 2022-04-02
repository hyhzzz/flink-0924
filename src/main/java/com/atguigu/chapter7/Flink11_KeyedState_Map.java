package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-31 19:05
 */
public class Flink11_KeyedState_Map {
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
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    private MapState<Integer, Object> vcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        vcState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Object>("vcState", Integer.class, Object.class));

                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        vcState.put(value.getVc(), new Object());

                        List<Integer> vcs = AtguiguUtil.toList(vcState.keys());
                        out.collect(ctx.getCurrentKey() + "不重复的水位值：" + vcs);

                    }
                }).print();
        //启动执行环境
        env.execute();
    }
}
