import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// shuffle
public class Example7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("shuffle: ").setParallelism(2);

        // 不会吧，竟然会覆盖本地
        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rebalance()
                .print("reb   alance: ").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("broadcase: ").setParallelism(2);

        env.execute();
    }
}
