package szp.rafael.rccar.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.flink.deserializers.BodyDeserializer;

import java.util.Random;

public class RCCarAssembler {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Random random = new Random();

        KafkaSource<Body> bodySource = KafkaSource.<Body>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("rccar-body")
                .setGroupId("rccar-body-group-"+random.nextLong(0,Long.MAX_VALUE))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new BodyDeserializer())
                .build();
        DataStream<Body> stream = env.fromSource(bodySource, WatermarkStrategy.noWatermarks(), "RC CAR Body Source");

        stream.print();
        env.execute("Flink avro rc assembler");




    }

}
