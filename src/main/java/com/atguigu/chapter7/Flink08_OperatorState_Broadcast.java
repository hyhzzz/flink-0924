package com.atguigu.chapter7;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author CoderHyh
 * @create 2022-03-31 19:05
 */
public class Flink08_OperatorState_Broadcast {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);

        /*
        广播状态使用的套路：
                1.需要两个流：一个是数据流，一个是配置流(数据流比较少) 配置流中的数据用来控制数据流中的数据的方式
                2.把配置流做成广播流
                3.让数据流去connect广播流
                4.把广播流中的数据放入到广播状态
                5.数据流中的数据处理的时候，从广播状态读取配置信息
         */


        BroadcastStream<String> bcStream = controlStream.broadcast(new MapStateDescriptor<String, String>("configState",String.class,String.class));

        dataStream.connect(bcStream)
                .process(new BroadcastProcessFunction<String, String, String>() {


                    //处理数据流的数据
                    @Override
                    public void processElement(String value,
                                               BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext,
                                               Collector<String> collector) throws Exception {
                        ReadOnlyBroadcastState<String, String> bcState = readOnlyContext.getBroadcastState(new MapStateDescriptor<String, String>("configState",String.class,String.class));

                        String config = bcState.get("switch");
                        if ("1".equals(bcState.get("switch"))) {
                            collector.collect("切换到1号配置....");
                        } else if ("0".equals(bcState.get("switch"))) {
                            collector.collect("切换到0号配置....");
                        } else {
                            collector.collect("切换到其他配置....");
                        }
                    }

                    //处理广播流的数据
                    @Override
                    public void processBroadcastElement(String value,
                                                        BroadcastProcessFunction<String, String, String>.Context context,
                                                        Collector<String> collector) throws Exception {


                        BroadcastState<String, String> bcState = context.getBroadcastState(new MapStateDescriptor<String, String>("configState",String.class,String.class));

                        bcState.put("switch", value);
                    }
                }).print();
        //启动执行环境
        env.execute();
    }
}
