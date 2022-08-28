package day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("生命周期开始，子任务索引是：" + getRuntimeContext().getIndexOfThisSubtask());
                    }

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        for (int i = 0; i < 10; i++) {
                            // 根据当前子任务（3个subtask）的索引进行数据的流动
                            if (i % 3 == getRuntimeContext().getIndexOfThisSubtask()) {
                                ctx.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        // 和run()配合使用，结束run()中的循环
                    }
                })
                .setParallelism(3)
                .print().setParallelism(2);

        env.execute();
    }
}
