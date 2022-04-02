package com.atguigu.chapter5.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author CoderHyh
 * @create 2022-03-29 14:50
 */
public class Flink03_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1607527992000L, 20),
                new WaterSensor("sensor_1", 1607527994000L, 50),
                new WaterSensor("sensor_1", 1607527996000L, 50),
                new WaterSensor("sensor_2", 1607527993000L, 10),
                new WaterSensor("sensor_2", 1607527995000L, 30)
        );

        FlinkJedisConfigBase redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();

        stream.keyBy(WaterSensor::getId)
                .sum("vc")
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<WaterSensor>() {
                    //返回命令描述符
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key,只对hash和zset有效，其他数据结构忽略
                        //String
                        //return new RedisCommandDescription(RedisCommand.SET, null);

                        //list
                        //return new RedisCommandDescription(RedisCommand.RPUSH, null);

                        //set
                        //return new RedisCommandDescription(RedisCommand.SADD, null);

                        //hash
                        return new RedisCommandDescription(RedisCommand.HSET, "sensor");

                    }

                    //数据要写入到redis
                    @Override
                    public String getKeyFromData(WaterSensor data) {
                        // 返回存在Redis中的数据类型  存储的是Hash, 第二个参数是外面的key
                        return data.getId(); //sensor id作为字符串的key
                    }

                    @Override
                    public String getValueFromData(WaterSensor data) {
                        // 从数据中获取Value: Hash的value
                        return JSON.toJSONString(data);
                    }
                }));

        env.execute();
    }
}
