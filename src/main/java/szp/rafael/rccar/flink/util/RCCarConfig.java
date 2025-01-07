package szp.rafael.rccar.flink.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class RCCarConfig {

    public static final String REGISTRY_URL = "http://localhost:8081";
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String RCCAR_BODY = "rccar-body";
    public static final String RCCAR_ENGINE = "rccar-engine";
    public static final String RCCAR_REMOTE_CONTROL = "rccar-remote-control";
    public static final String RCCAR_WHEEL = "rccar-wheel";

    public static Properties kafkaProperties(String groupId){
        var kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return kafkaProps;
    }

}
