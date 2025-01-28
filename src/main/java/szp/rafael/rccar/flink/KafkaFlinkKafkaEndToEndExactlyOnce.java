package szp.rafael.rccar.flink;

import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Desc
 * Kafka --> Flink-->Kafka  End to end exactly once
 * Direct use
 * FlinkKafkaConsumer  +  Flink Checkpoint + flinkkafkaproducer
 * Credits: https://programming.vip/docs/flink-53-end-to-end-exactly-once-the-advanced-feature-of-flink.html
 */
public class KafkaFlinkKafkaEndToEndExactlyOnce {

    public static final String BROKER = "broker:19092";

    public static void main(String[] args) throws Exception {
        //1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //===========Checkpoint parameter setting====
        //===========Type 1: required parameter=============
        //Set the time interval of Checkpoint to 1000ms and do Checkpoint once / in fact, send Barrier every 1000ms!
        env.enableCheckpointing(1000);
        //Set State storage media
        if (SystemUtils.IS_OS_LINUX) {
            env.setStateBackend(new FsStateBackend("file:///tmp/ckp"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint"));
        }
        //===========Type 2: recommended parameters===========
        //Set the minimum waiting time between two checkpoints. For example, set the minimum waiting time between checkpoints to be 500ms (in order to avoid that the previous time is too slow and the latter time overlaps when doing Checkpoint every 1000ms)
        //For example, on the expressway, one vehicle is released at the gate every 1s, but the minimum distance between two vehicles is 500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//The default is 0
        //Set whether to fail the overall task if an error occurs in the process of Checkpoint: true is false, not true
        //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);// The default is true
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);//The default value is 0, which means that any checkpoint failure is not tolerated
        //Set whether to clear checkpoints, indicating whether to keep the current Checkpoint when canceling. The default Checkpoint will be deleted when the job is cancelled
        //ExternalizedCheckpointCleanup. DELETE_ ON_ Cancelation: true. When the job is cancelled, the external checkpoint is deleted (the default value)
        //ExternalizedCheckpointCleanup. RETAIN_ ON_ Cancel: false. When the job is cancelled, the external checkpoint will be reserved
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //===========Type 3: just use the default===============
        //Set the execution mode of checkpoint to actual_ Once (default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //Set the timeout time of the Checkpoint. If the Checkpoint has not been completed within 60s, it means that the Checkpoint fails, it will be discarded.
        env.getCheckpointConfig().setCheckpointTimeout(60000);//Default 10 minutes
        //Set how many checkpoint s can be executed at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//The default is 1

        //=============Restart strategy===========
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.of(3, TimeUnit.SECONDS)));

        //2.Source
        Properties props_source = new Properties();
        props_source.setProperty("bootstrap.servers", BROKER);
        props_source.setProperty("group.id", "flink");
        props_source.setProperty("auto.offset.reset", "latest");
        props_source.setProperty("flink.partition-discovery.interval-millis", "5000");//A background thread will be started to check the partition of Kafka every 5s
        //props_source.setProperty("enable.auto.commit", "true");// When there is no Checkpoint, use the auto submit offset to the default theme:__ consumer_ In offsets
        //props_source.setProperty("auto.commit.interval.ms", "2000");
        //kafkaSource is KafkaConsumer
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>("flink_kafka1", new SimpleStringSchema(), props_source);
//        kafkaSource.setStartFromLatest();
        kafkaSource.setStartFromGroupOffsets();// Set to start consumption from the offset of the record. If there is no record, start from auto offset. Reset configuration starts consumption
        //kafkaSource.setStartFromEarliest();// Set direct consumption from Earliest, and auto offset. Reset configuration independent
        kafkaSource.setCommitOffsetsOnCheckpoints(true);//When executing Checkpoint, submit offset to Checkpoint (for Flink), and submit a copy to the default theme:__ consumer_ Offsets (it can also be obtained if other external systems want to use it)
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3.Transformation
        //3.1 cut out each word and write it down as 1
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                Logger logger = LoggerFactory.getLogger(this.getClass());
                //Each line is value
                logger.info("Reading {}",value);
                String[] words = value.split(" ");
                Random random = new SecureRandom(String.valueOf(System.currentTimeMillis()).getBytes());
                long i =  (System.currentTimeMillis() + random.nextInt(1007)) % 17;  // random.nextInt(10);
                logger.warn("Random {}",random);
                for (String word : words) {
                    if (i == 0) {
                        System.out.println("Out bug Yes..."+i);
                        throw new RuntimeException("Out bug Yes..."+i);
                    }
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //3.2 grouping
        //Note: the grouping of batch processing is groupBy and the grouping of stream processing is keyBy
        KeyedStream<Tuple2<String, Integer>, Tuple> groupedDS = wordAndOneDS.keyBy(0);
        //3.3 polymerization
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggResult = groupedDS.sum(1);
        //3.4 convert the aggregation results to custom string format
        SingleOutputStreamOperator<String> result = (SingleOutputStreamOperator<String>) aggResult.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0 + ":::" + value.f1;
            }
        });

        //4.sink
        //result.print();
        Properties props_sink = new Properties();
        props_sink.setProperty("bootstrap.servers", BROKER);
        props_sink.setProperty("transaction.timeout.ms", 1000 * 5 + "");//Set the transaction timeout, which can also be set in kafka configuration
        /*FlinkKafkaProducer<String> kafkaSink0 = new FlinkKafkaProducer<>(
                "flink_kafka",
                new SimpleStringSchema(),
                props_sink);*/
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>(
                "flink_kafka3",
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                props_sink,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
        result.addSink(kafkaSink);

        //5.execute
        env.execute();
        //Test:
        //1. Create theme / export / server / Kafka / bin / Kafka topics sh --zookeeper node1:2181 --create --replication-factor 2 --partitions 3 --topic flink_ kafka2
        //2. Open console producer / export / server / Kafka / bin / Kafka console producer sh --broker-list node1:9092 --topic flink_ kafka
        //3. Open the console / export / server / Kafka / bin / Kafka console consumer sh --bootstrap-server node1:9092 --topic flink_ kafka2
    }
}