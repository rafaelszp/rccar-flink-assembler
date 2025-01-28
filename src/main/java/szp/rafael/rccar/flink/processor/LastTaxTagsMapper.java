package szp.rafael.rccar.flink.processor;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.LastTaxTags;
import szp.rafael.rccar.dto.TaxTag;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class LastTaxTagsMapper extends RichFlatMapFunction<TaxTag, LastTaxTags> {

    transient MapState<String, LinkedList<TaxTag>> lastThreeTaxTagsMapState;
    transient Logger logger;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        lastThreeTaxTagsMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-three-tax-tags-map",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<>() {
                })));

        logger = LoggerFactory.getLogger(LastTaxTagsMapper.class);
    }

    @Override
    public void flatMap(TaxTag taxTag, Collector<LastTaxTags> collector) throws Exception {

        add(taxTag);
        LastTaxTags.Builder taxTagsBuilder = LastTaxTags.newBuilder().setStateName(taxTag.getState().name()).setTaxTags(new ArrayList<>());
        List<TaxTag> taxTags = lastThreeTaxTagsMapState.get(taxTag.getState().name());
        taxTagsBuilder.getTaxTags().addAll(taxTags);
        collector.collect(taxTagsBuilder.build());
    }

    public void add(TaxTag taxTag) throws Exception {
        var key = taxTag.getState().name();


        LinkedList<TaxTag> taxTags = lastThreeTaxTagsMapState.get(key);
        if(taxTags == null){
            taxTags = new LinkedList<>();
        }
        CircularFifoQueue lastTaxTags = new CircularFifoQueue(3);

        lastTaxTags.addAll(taxTags);

        logger.info("current tag {}",taxTag);

        lastTaxTags.offer(taxTag);
        taxTags.clear();
        taxTags.addAll(lastTaxTags);
        lastThreeTaxTagsMapState.put(key,taxTags);
    }

}
