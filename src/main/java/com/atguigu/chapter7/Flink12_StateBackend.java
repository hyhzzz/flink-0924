package com.atguigu.chapter7;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author coderhyh
 * @create 2022-04-01 13:03
 */
class Flink12_StateBackend {
    public static void main(String[] args) throws Exception {
        //获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //开启checkpoint
        env.enableCheckpointing(2000);


        System.setProperty("HADOOP_USER_NAME", "atguigu");


        //内存
        //1.12 旧的写法
        //env.setStateBackend(new MemoryStateBackend());
        //1.13 本地 新的写法
        //env.setStateBackend(new HashMapStateBackend());
        //1.13 远程 新的写法
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());


        //fs
        //1.12 旧的写法
        //env.setStateBackend(new FsStateBackend(""));
        //1.13 本地 新的写法
        env.setStateBackend(new HashMapStateBackend());
        //1.13 远程 新的写法
        //env.getCheckpointConfig().setCheckpointStorage("");


        //rocksdb
        //1.12 旧的写法
        //env.setStateBackend(new RocksDBStateBackend(""));
        //1.13 本地 新的写法
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //1.13 远程 新的写法
        //env.getCheckpointConfig().setCheckpointStorage("");


        //启动执行环境
        env.execute();
    }
}
