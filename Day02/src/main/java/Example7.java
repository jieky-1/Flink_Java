import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// shuffle
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 上游完全随机发送数据下游算子的subtask
        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("shuffle: ").setParallelism(3);

        // 随机轮询发送数据，下游算子并行度为3，将元素轮询着发给下游算子的3个subtask
        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rebalance()
                .print("rebalance: ").setParallelism(3);

        // 给下游算子所有的subtask都广播一份数据，并行度为2，广播两份
        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("broadcase: ").setParallelism(2);

        env.execute();
    }
}
