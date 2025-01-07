package szp.rafael.rccar.flink.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.RemoteControl;
import szp.rafael.rccar.dto.Wheel;
import szp.rafael.rccar.flink.deserializers.AvroDeserializer;

import java.util.Random;

public class RCCarStreamFactory {

    private static Random random = new Random();

    public static DataStream<Body> createBodyStream(StreamExecutionEnvironment env) {

        KafkaSource<Body> bodySource = KafkaSource.<Body>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_BODY)
                .setGroupId(RCCarConfig.RCCAR_BODY+"-group-"+random.nextLong(0,Long.MAX_VALUE))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(AvroDeserializer.create(Body.class, RCCarConfig.REGISTRY_URL))
                .build();
        return env.fromSource(bodySource, WatermarkStrategy.noWatermarks(), "RC CAR Body Source").keyBy(body -> body.getPart().getSku());
    }

    public static DataStream<Engine> createEngineStream(StreamExecutionEnvironment env) {
        KafkaSource<Engine> engineSource = KafkaSource.<Engine>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_ENGINE)
                .setGroupId(RCCarConfig.RCCAR_ENGINE+"-group-"+random.nextLong(0,Long.MAX_VALUE))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(AvroDeserializer.create(Engine.class, RCCarConfig.REGISTRY_URL))
                .build();
        return env.fromSource(engineSource, WatermarkStrategy.noWatermarks(), "RC CAR Engine Source").keyBy(engine -> engine.getPart().getSku());
    }

    public static DataStream<RemoteControl> createRemoteControlStream(StreamExecutionEnvironment env) {
        KafkaSource<RemoteControl> remoteControlSource = KafkaSource.<RemoteControl>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_REMOTE_CONTROL)
                .setGroupId(RCCarConfig.RCCAR_REMOTE_CONTROL+"-group-"+random.nextLong(0,Long.MAX_VALUE))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(AvroDeserializer.create(RemoteControl.class, RCCarConfig.REGISTRY_URL))
                .build();
        return env.fromSource(remoteControlSource, WatermarkStrategy.noWatermarks(), "RC CAR Remote Control Source").keyBy(remoteControl -> remoteControl.getPart().getSku());
    }

    public static DataStream<Wheel> createWheelStream(StreamExecutionEnvironment env) {
        KafkaSource<Wheel> wheelSource = KafkaSource.<Wheel>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_WHEEL)
                .setGroupId(RCCarConfig.RCCAR_WHEEL+"-group-"+random.nextLong(0,Long.MAX_VALUE))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(AvroDeserializer.create(Wheel.class, RCCarConfig.REGISTRY_URL))
                .build();
        return env.fromSource(wheelSource, WatermarkStrategy.noWatermarks(), "RC CAR Wheel Source").keyBy(wheel -> wheel.getPart().getSku());
    }

}
