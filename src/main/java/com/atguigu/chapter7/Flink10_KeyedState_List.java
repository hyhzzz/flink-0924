package com.atguigu.chapter7;

import com.atguigu.bean.WaterSensor;
import com.atguigu.util.AtguiguUtil;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;

/**
 * @author CoderHyh
 * @create 2022-03-31 19:05
 */
public class Flink10_KeyedState_List {
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

                    private ListState<Integer> top3VcState;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        top3VcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Integer.class));
                        //getRuntimeContext().getListState(new ListStateDescriptor<Integer>("top3VcState", Types.INT){});

                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        top3VcState.add(value.getVc());
                        List<Integer> list = AtguiguUtil.toList(top3VcState.get());
                        //list.sort(new Comparator<Integer>() {
                        //    @Override
                        //    public int compare(Integer o1, Integer o2) {
                        //        return o2.compareTo(o1);
                        //    }
                        //});

                        //list.sort((o1, o2) -> o2.compareTo(o1));

                        list.sort(Comparator.reverseOrder()); //对list中的元素进行排序

                        if (list.size() > 3) {
                            list.remove(list.size() - 1);
                        }
                        out.collect("top3" + list);

                        //更新top3到状态
                        top3VcState.update(list);
                    }
                }).print();
        //启动执行环境
        env.execute();
    }
}
