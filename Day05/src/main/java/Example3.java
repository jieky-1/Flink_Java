import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Example3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.emitWatermark(new Watermark(999L));
                        ctx.collectWithTimestamp("b", 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp("c", 4000L);
                        ctx.emitWatermark(new Watermark(4999L));
                        ctx.collectWithTimestamp("d", 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> 1)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 以匿名内部类的形式传递侧输出的名字
                .sideOutputLateData(new OutputTag<String>("late") {})
                // 由于手动生成水位线和带指定时间戳的数据，assignTimestampsAndWatermarks可不调用
                .process(new ProcessWindowFunction<String, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        out.collect("窗口中共有：" + elements.spliterator().getExactSizeIfKnown());
                    }
                });

        result.print();

        // 侧输出标签是单例模式：实例化两次，说明侧输出标签是单例
        result.getSideOutput(new OutputTag<String>("late"){}).print("late: ");

        env.execute();
    }
}
