package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.LastTaxTags;

import java.security.SecureRandom;
import java.util.Random;

public class LastTaxTagsSlowProcessor extends KeyedProcessFunction<CharSequence, LastTaxTags, LastTaxTags> {

    transient ValueState<Long> countState;
    transient MapState<String, Boolean> errorState;
    transient Logger logger;


    @Override
    public void open(OpenContext openContext) throws Exception {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public LastTaxTagsSlowProcessor() {
    }

    @Override
    public void processElement(LastTaxTags lastTaxTags, KeyedProcessFunction<CharSequence, LastTaxTags, LastTaxTags>.Context context, Collector<LastTaxTags> collector) throws Exception {
        Thread.sleep(new Random().nextInt(200));
        randomErrorGenerator(lastTaxTags.getStateName().toString());
        collector.collect(lastTaxTags);
        logger.info("Collected last tax tags: {}", lastTaxTags);
    }

    private void randomErrorGenerator(String key) {
        Random random = new SecureRandom((System.currentTimeMillis()+key).getBytes());
        long numerator = System.currentTimeMillis() + random.nextInt(1007);
        int divisor = random.nextInt(17, 109);
        long i = numerator % divisor;
        logger.warn("Random {}", random);

        if (i == 0) {
            String msg = String.format("Random error simulation: %s... %s / %s",i,numerator,divisor);
            logger.error(msg);
            throw new RuntimeException(msg);
        }
    }
}
