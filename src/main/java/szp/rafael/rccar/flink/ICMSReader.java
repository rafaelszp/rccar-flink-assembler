package szp.rafael.rccar.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.TaxTag;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;
import szp.rafael.rccar.flink.processor.LastTaxTagsMapper;

public class ICMSReader {

    private static final Logger logger = LoggerFactory.getLogger(ICMSReader.class);


    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.createLocalEnvironment();


        DataStream<TaxTag> taxTagStream = RCCarStreamFactory.createTaxTagStream(env);

        taxTagStream.flatMap(new LastTaxTagsMapper()).print();


        env.execute("ICMS Reader");

    }

}
