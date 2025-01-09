package szp.rafael.rccar.flink;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.factory.KafkaSinkFactory;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;
import szp.rafael.rccar.flink.function.AsyncPriceCollector;
import szp.rafael.rccar.flink.processor.BodyEngineJoin;
import szp.rafael.rccar.flink.processor.RCCarRemoteControlJoin;
import szp.rafael.rccar.flink.processor.RCCarWheelsJoin;
import szp.rafael.rccar.flink.util.RCCarConfig;

import java.util.concurrent.TimeUnit;

public class RCCarAssembler {


    private static final Logger logger = LoggerFactory.getLogger(RCCarAssembler.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.createLocalEnvironment();

        var bodyStream = RCCarStreamFactory.createBodyStream(env);
        var engineStream = RCCarStreamFactory.createEngineStream(env);
        var remoteControlStream = RCCarStreamFactory.createRemoteControlStream(env);
        var wheelStream = RCCarStreamFactory.createWheelStream(env);


        var rccarStream = bodyStream.connect(engineStream)
                .keyBy(b -> b.getPart().getSku(), e -> e.getPart().getSku())
                .process(new BodyEngineJoin())
                .connect(remoteControlStream)
                .keyBy(RCCar::getSku, r -> r.getPart().getSku())
                .process(new RCCarRemoteControlJoin())
                .connect(wheelStream)
                .keyBy(RCCar::getSku, w -> w.getPart().getSku())
                .process(new RCCarWheelsJoin())
                .map(rccar -> {
                    logger.info("rccar: {}", rccar);
                    return rccar;
                }).keyBy(RCCar::getSku);

        AsyncDataStream.unorderedWait(rccarStream,new AsyncPriceCollector(),1000, TimeUnit.SECONDS,10);



        KafkaSink<RCCar> completeSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_COMPLETE);
        KafkaSink<RCCar> missingPartsSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_INCOMPLETE);

        rccarStream.filter(rccar -> rccar.getSituation().equals(CarSituation.COMPLETE))
                .sinkTo(completeSink);

        rccarStream.filter(rccar -> !rccar.getSituation().equals(CarSituation.COMPLETE))
                .sinkTo(missingPartsSink);

        env.execute("Flink avro rc assembler");

    }

}
