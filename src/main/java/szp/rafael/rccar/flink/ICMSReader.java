package szp.rafael.rccar.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.LastTaxTags;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.dto.TaxTag;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;
import szp.rafael.rccar.flink.processor.LastTaxTagsMapper;
import szp.rafael.rccar.flink.processor.LastTaxTagsSlowProcessor;
import szp.rafael.rccar.flink.util.RCCarConfig;

public class ICMSReader {

    private static final Logger logger = LoggerFactory.getLogger(ICMSReader.class);


    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env;
        DataStream<TaxTag> taxTagStream;

        if (args.length == 0 && params.has("dev")) {

            env = StreamExecutionEnvironmentFactory.createLocalEnvironment();

            taxTagStream = RCCarStreamFactory.createTaxTagStream(env, true);
        } else {
            env = StreamExecutionEnvironmentFactory.createLocalEnvironment(params);
            taxTagStream = RCCarStreamFactory.createTaxTagStream(env, false, params);
        }

        KafkaSink<LastTaxTags> sink = createSink(params);

        taxTagStream.flatMap(new LastTaxTagsMapper()).keyBy(LastTaxTags::getStateName)
                .process(new LastTaxTagsSlowProcessor())
                .sinkTo(sink);

        env.execute("ICMS Reader");

    }

    public static KafkaSink<LastTaxTags> createSink(ParameterTool params){
        String topic = "last-tax-tags";

        String brokers = params.get(RCCarStreamFactory.KAFKA_BROKER_PARAM,"broker:19092");
        String registryUrl = params.get(RCCarStreamFactory.REGISTRY_URL_PARAM,"http://localhost:8081");
        return KafkaSink.<LastTaxTags>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(ConfluentRegistryAvroSerializationSchema.forSpecific(LastTaxTags.class, LastTaxTags.class.getName(), registryUrl))
                        .setKeySerializationSchema(tags -> tags.getStateName().toString().getBytes())
                        .setPartitioner(new FlinkFixedPartitioner<LastTaxTags>())

                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setProperty("transaction.timeout.ms", "10000")
                .build();
    }

}
