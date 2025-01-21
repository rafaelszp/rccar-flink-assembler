package szp.rafael.rccar.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.retryable.AsyncRetryStrategies;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.enums.PartType;
import szp.rafael.rccar.flink.factory.KafkaSinkFactory;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;
import szp.rafael.rccar.flink.function.AsyncPriceCollector;
import szp.rafael.rccar.flink.function.RCcarFlatMap;
import szp.rafael.rccar.flink.processor.BodyEngineJoin;
import szp.rafael.rccar.flink.processor.RCCarRemoteControlJoin;
import szp.rafael.rccar.flink.processor.RCCarWheelsJoin;
import szp.rafael.rccar.flink.util.RCCarConfig;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RCCarAssembler {


    private static final Logger logger = LoggerFactory.getLogger(RCCarAssembler.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.createLocalEnvironment();

        var rccarStream = getRccarStream(env);

        SingleOutputStreamOperator<RCCar> awaitingPriceStream = rccarStream.filter(rccar -> rccar.getSituation().equals(CarSituation.AWAITING_PRICE));

        int timeout = 10 * 3;
        KeyedStream<RCCar, CharSequence> pricedBodyStream = getPricedSteam(awaitingPriceStream, timeout);

        KafkaSink<RCCar> completeSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_COMPLETE);
        KafkaSink<RCCar> missingPartsSink = KafkaSinkFactory.createKafkaSink(RCCarConfig.RCCAR_INCOMPLETE);

        pricedBodyStream.filter(rccar -> rccar.getSituation().equals(CarSituation.COMPLETE))
                .sinkTo(completeSink);

        pricedBodyStream.filter(rcCar -> !rcCar.getSituation().equals(CarSituation.COMPLETE))
                .sinkTo(missingPartsSink);

        rccarStream.filter(rccar -> rccar.getSituation().name().contains("MISSING"))
                .sinkTo(missingPartsSink);


        env.execute("Flink avro rc assembler");

    }

    private static KeyedStream<RCCar, CharSequence>  getPricedSteam(SingleOutputStreamOperator<RCCar> awaitingPriceStream, int timeout){
        KeyedStream<RCCar, CharSequence> pricedBodyStream = getAsyncStream(PartType.BODY, awaitingPriceStream, timeout);
        KeyedStream<RCCar, CharSequence> pricedEngineStream = getAsyncStream(PartType.ENGINE, awaitingPriceStream, timeout);
        KeyedStream<RCCar, CharSequence> pricedRCStream = getAsyncStream(PartType.REMOTE_CONTROL, awaitingPriceStream, timeout);
        KeyedStream<RCCar, CharSequence> pricedWheelStream = getAsyncStream(PartType.WHEEL, awaitingPriceStream, timeout);
        pricedBodyStream
                .connect(pricedEngineStream).flatMap(new RCcarFlatMap(PartType.ENGINE)).keyBy(RCCar::getSku)
                .connect(pricedRCStream).flatMap(new RCcarFlatMap(PartType.REMOTE_CONTROL)).keyBy(RCCar::getSku)
                .connect(pricedWheelStream).flatMap(new RCcarFlatMap(PartType.WHEEL)).keyBy(RCCar::getSku);
        return pricedBodyStream;
    }


    private static KeyedStream<RCCar, CharSequence> getAsyncStream(PartType partType, SingleOutputStreamOperator<RCCar> awaitingPriceStream, int timeout) {
        SingleOutputStreamOperator<RCCar> singleOutputStreamOperator = AsyncDataStream.unorderedWaitWithRetry(awaitingPriceStream, new AsyncPriceCollector(partType), timeout, TimeUnit.SECONDS, 5, createAsyncStrategy());
        KeyedStream<RCCar, CharSequence> output = singleOutputStreamOperator.keyBy(RCCar::getSku);
        return output;
    }

    private static DataStream<RCCar> getRccarStream(StreamExecutionEnvironment env) {

        var bodyStream = RCCarStreamFactory.createBodyStream(env);
        var engineStream = RCCarStreamFactory.createEngineStream(env);
        var remoteControlStream = RCCarStreamFactory.createRemoteControlStream(env);
        var wheelStream = RCCarStreamFactory.createWheelStream(env);

        return bodyStream.connect(engineStream)
                .keyBy(b -> b.getPart().getSku(), e -> e.getPart().getSku())
                .process(new BodyEngineJoin())
                .connect(remoteControlStream)
                .keyBy(RCCar::getSku, r -> r.getPart().getSku())
                .process(new RCCarRemoteControlJoin())
                .connect(wheelStream)
                .keyBy(RCCar::getSku, w -> w.getPart().getSku())
                .process(new RCCarWheelsJoin())
                .map(rccar -> {
//                    logger.info("rccar: {}", rccar);
                    return rccar;
                }).keyBy(RCCar::getSku);
    }

    private static AsyncRetryStrategies.FixedDelayRetryStrategy createAsyncStrategy() {
        return new AsyncRetryStrategies.FixedDelayRetryStrategyBuilder(3, 1000 * 10)
                .ifResult(RetryPredicates.EMPTY_RESULT_PREDICATE)
                .ifException(RetryPredicates.HAS_EXCEPTION_PREDICATE)
                .build();
    }

}
