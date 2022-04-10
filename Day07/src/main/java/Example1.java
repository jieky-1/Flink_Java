import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

// 写入kafka
// 驱动卸载后面，但是在日志驱动前面
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092");

        env
                .readTextFile("E:\\BigData200105\\Flink_Java\\Day07\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-1",
                        // 往kafka中写的数据类型
                        new SimpleStringSchema(),
                        properties
                ));

        env.execute();
    }
}
