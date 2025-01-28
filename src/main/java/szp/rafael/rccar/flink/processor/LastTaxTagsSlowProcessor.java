package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.LastTaxTags;

public class LastTaxTagsSlowProcessor extends KeyedProcessFunction<CharSequence, LastTaxTags, LastTaxTags> {

    transient ValueState<Long> countState;
    transient MapState<String,Boolean> errorState;
    transient Logger logger;


    @Override
    public void open(OpenContext openContext) throws Exception {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public LastTaxTagsSlowProcessor() {
    }

    @Override
    public void processElement(LastTaxTags lastTaxTags, KeyedProcessFunction<CharSequence, LastTaxTags, LastTaxTags>.Context context, Collector<LastTaxTags> collector) throws Exception {
        Thread.sleep(1500);
        collector.collect(lastTaxTags);
        logger.info("Collected last tax tags: {}",lastTaxTags);
    }
}
