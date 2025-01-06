package szp.rafael.rccar.flink.deserializers;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public abstract class AbstractDeserializer<T extends org.apache.avro.specific.SpecificRecord> implements KafkaRecordDeserializationSchema<T> {


    protected Class<T> clazz;

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(clazz);
    }

    public AbstractDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) throws IOException {
        try {
            ConfluentRegistryAvroDeserializationSchema<T> ds = ConfluentRegistryAvroDeserializationSchema.forSpecific(clazz, "http://localhost:8081");
            T entity = ds.deserialize(consumerRecord.value());

            collector.collect(entity);
        } catch (Exception e) {
            throw new RuntimeException("Erro ao deserializar registro Avro", e);
        }
    }

}
