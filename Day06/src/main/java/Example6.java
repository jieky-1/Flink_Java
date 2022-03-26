package day06;

import day03.Example9;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example6 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new FsStateBackend("file:///home/zuoyuan/flink0224tutorial/src/main/resources/ckpt", false));
        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(new Example9.ClickSource())
                .print();

        env.execute();
    }
}
