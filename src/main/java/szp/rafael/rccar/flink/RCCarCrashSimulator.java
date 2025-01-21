package szp.rafael.rccar.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class RCCarCrashSimulator {
    private static final Logger logger = LoggerFactory.getLogger(RCCarCrashSimulator.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.createLocalEnvironment();

        var bodyStream = RCCarStreamFactory.createBodyStream(env, "CRASH_SIM_THROW_3").map(new MapFunction<Body, Body>() {
            AtomicInteger count = new AtomicInteger(0);

            @Override
            public Body map(Body body) throws Exception {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {}
                if (count.incrementAndGet() >= 50) {
                    logger.warn("SIMULANDO CRASH");
                    throw new RuntimeException("CRASH SIM");
                }

                return body;
            }
        }).keyBy(b -> b.getPart().getSku());

        bodyStream.process(new ProcessFunction<Body, Body>() {

            transient ValueState<Body> previousState;

            @Override
            public void open(OpenContext openContext) throws Exception {
                super.open(openContext);

                previousState = getRuntimeContext().getState(new ValueStateDescriptor<Body>("current-state", Body.class));

            }

            @Override
            public void processElement(Body body, ProcessFunction<Body, Body>.Context context, Collector<Body> collector) throws Exception {
                Body previous = previousState.value();
                if (previous == null || !previous.getPart().getSku().equals(body.getPart().getSku())) {
                    logger.info("Body anterior: " + previous);
                    logger.info("Body atual: " + body);
                    body.setColor("RUNNAWAY_DONKEY");
                    previousState.update(body);
                }
                collector.collect(body);
            }
        }).print();


        env.execute("RCCar Crash Simulator");


    }
}
