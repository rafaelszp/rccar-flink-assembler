package szp.rafael.rccar.flink.factory;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.RemoteControl;
import szp.rafael.rccar.dto.TaxTag;
import szp.rafael.rccar.dto.Wheel;
import szp.rafael.rccar.flink.serdes.AvroDeserializer;
import szp.rafael.rccar.flink.util.RCCarConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;

public class RCCarStreamFactory {

    public static final OffsetsInitializer STARTING_OFFSETS_INITIALIZER = OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
    public static final String KAFKA_BROKER_PARAM = "brokers";
    public static final String REGISTRY_URL_PARAM = "registry-url";
//    public static final OffsetsInitializer STARTING_OFFSETS_INITIALIZER = OffsetsInitializer.latest();
    private static Random random = new Random();

    public static DataStream<Body> createBodyStream(StreamExecutionEnvironment env) {

        //kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        String groupId = RCCarConfig.RCCAR_BODY + "-group-" + getaLong();
        KafkaSource<Body> bodySource = KafkaSource.<Body>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_BODY)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(Body.class, RCCarConfig.REGISTRY_URL))
                .setProperties(RCCarConfig.kafkaProperties(groupId))
                .build();
        return env.fromSource(bodySource, WatermarkStrategy.noWatermarks(), "RC CAR Body Source").keyBy(body -> body.getPart().getSku());
    }
    public static DataStream<Body> createBodyStream(StreamExecutionEnvironment env,String groupId) {
        //kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaSource<Body> bodySource = KafkaSource.<Body>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_BODY)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(Body.class, RCCarConfig.REGISTRY_URL))
                .setProperties(RCCarConfig.kafkaProperties(groupId))
                .build();
        return env.fromSource(bodySource, WatermarkStrategy.noWatermarks(), "RC CAR Body Source").keyBy(body -> body.getPart().getSku());
    }

    public static DataStream<Engine> createEngineStream(StreamExecutionEnvironment env) {
        String groupId = RCCarConfig.RCCAR_ENGINE + "-group-" + getaLong();
        KafkaSource<Engine> engineSource = KafkaSource.<Engine>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_ENGINE)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(Engine.class, RCCarConfig.REGISTRY_URL))
                .setProperties(RCCarConfig.kafkaProperties(groupId))
                .build();
        return env.fromSource(engineSource, WatermarkStrategy.noWatermarks(), "RC CAR Engine Source").keyBy(engine -> engine.getPart().getSku());
    }

    private static long getaLong() {
        SimpleDateFormat formatador = new SimpleDateFormat("yyyyMMdd", Locale.getDefault());
        String dataFormatada = formatador.format(new Date());
        return Long.valueOf(dataFormatada);
//
//        return random.nextLong(0, Long.MAX_VALUE);
    }

    public static DataStream<RemoteControl> createRemoteControlStream(StreamExecutionEnvironment env) {
        String groupId = RCCarConfig.RCCAR_REMOTE_CONTROL + "-group-" + getaLong();
        KafkaSource<RemoteControl> remoteControlSource = KafkaSource.<RemoteControl>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_REMOTE_CONTROL)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(RemoteControl.class, RCCarConfig.REGISTRY_URL))
                .setProperties(RCCarConfig.kafkaProperties(groupId))
                .build();
        return env.fromSource(remoteControlSource, WatermarkStrategy.noWatermarks(), "RC CAR Remote Control Source").keyBy(remoteControl -> remoteControl.getPart().getSku());
    }

    public static DataStream<Wheel> createWheelStream(StreamExecutionEnvironment env) {
        String groupId = RCCarConfig.RCCAR_WHEEL + "-group-" + getaLong();
        KafkaSource<Wheel> wheelSource = KafkaSource.<Wheel>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.RCCAR_WHEEL)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(Wheel.class, RCCarConfig.REGISTRY_URL))
                .setProperties(RCCarConfig.kafkaProperties(groupId))
                .build();
        return env.fromSource(wheelSource, WatermarkStrategy.noWatermarks(), "RC CAR Wheel Source").keyBy(wheel -> wheel.getPart().getSku());
    }

    public static DataStream<TaxTag> createTaxTagStream(StreamExecutionEnvironment env, boolean uniqueGroupId) {

        String groupId = RCCarConfig.TAXTAG + "-group-" + getaLong();
        if(uniqueGroupId){
            groupId = groupId+(System.currentTimeMillis());
        }
        Properties props = RCCarConfig.kafkaProperties(groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaSource<TaxTag> tagSource = KafkaSource.<TaxTag>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(RCCarConfig.TAXTAG)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(TaxTag.class, RCCarConfig.REGISTRY_URL))
                .setProperties(props)
                .build();
        return env.fromSource(tagSource, WatermarkStrategy.noWatermarks(), "TaxTag Source").keyBy(tx -> tx.getState().name());
    }

    public static DataStream<TaxTag> createTaxTagStream(StreamExecutionEnvironment env, boolean uniqueGroupId, ParameterTool params) {

        String groupId = RCCarConfig.TAXTAG + "-group-" + getaLong();
        if(uniqueGroupId){
            groupId = groupId+(System.currentTimeMillis());
        }
        String brokers = params.get(KAFKA_BROKER_PARAM,"localhost:19092");
        String registryUrl = params.get(REGISTRY_URL_PARAM,"http://localhost:8081");
        Properties props = RCCarConfig.kafkaProperties(groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaSource<TaxTag> tagSource = KafkaSource.<TaxTag>builder()
                .setBootstrapServers(brokers)
                .setTopics(RCCarConfig.TAXTAG)
                .setGroupId(groupId)
                .setStartingOffsets(STARTING_OFFSETS_INITIALIZER)
                .setDeserializer(AvroDeserializer.create(TaxTag.class, registryUrl))
                .setProperties(props)
                .build();
        return env.fromSource(tagSource, WatermarkStrategy.noWatermarks(), "TaxTag Source").keyBy(tx -> tx.getState().name());
    }

}
