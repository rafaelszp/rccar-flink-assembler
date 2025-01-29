package szp.rafael.rccar.flink.factory;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.util.RCCarConfig;

public class KafkaSinkFactory {

    public static KafkaSink<RCCar> createKafkaSink(final String topic) {
        return KafkaSink.<RCCar>builder()
                .setBootstrapServers(RCCarConfig.KAFKA_BOOTSTRAP_SERVERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(RCCar.class, RCCar.class.getName(), RCCarConfig.REGISTRY_URL))
                        .setKeySerializationSchema(rcCar -> rcCar.getSku().toString().getBytes())
                        .setPartitioner(new FlinkFixedPartitioner<RCCar>())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();
    }

}
