package szp.rafael.rccar.flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.LastTaxTags;
import szp.rafael.rccar.dto.TaxTag;
import szp.rafael.rccar.flink.factory.RCCarStreamFactory;
import szp.rafael.rccar.flink.factory.StreamExecutionEnvironmentFactory;
import szp.rafael.rccar.flink.processor.LastTaxTagsMapper;
import szp.rafael.rccar.flink.processor.LastTaxTagsSlowProcessor;

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
        taxTagStream.flatMap(new LastTaxTagsMapper()).keyBy(LastTaxTags::getStateName)
                .process(new LastTaxTagsSlowProcessor())
                .print("Last-Tax-Tags");


        env.execute("ICMS Reader");

    }

}
