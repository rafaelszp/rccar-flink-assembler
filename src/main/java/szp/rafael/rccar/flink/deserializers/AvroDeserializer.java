package szp.rafael.rccar.flink.deserializers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class AvroDeserializer<T extends org.apache.avro.specific.SpecificRecord> implements KafkaRecordDeserializationSchema<T> {


    protected Class<T> clazz;
    protected String registryUrl;

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(clazz);
    }

    public AvroDeserializer(Class<T> clazz,String registryUrl) {
        this.clazz = clazz;
        this.registryUrl = registryUrl;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) throws IOException {
        try {
            ConfluentRegistryAvroDeserializationSchema<T> ds = ConfluentRegistryAvroDeserializationSchema.forSpecific(clazz, this.registryUrl);
            T entity = ds.deserialize(consumerRecord.value());

            collector.collect(entity);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao deserializar registro Avro", e);
        }
    }

    public static <T extends org.apache.avro.specific.SpecificRecord> AvroDeserializer<T> create(Class<T> clazz, String schemaRegistryUrl) {
        return new AvroDeserializer<T>(clazz, schemaRegistryUrl) {
        };
    }

}
