package szp.rafael.rccar.flink.processor;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.util.Collector;
import szp.rafael.rccar.dto.LastTaxTags;
import szp.rafael.rccar.dto.TaxTag;

import java.util.ArrayList;

public class LastTaxTagsMapper extends RichFlatMapFunction<TaxTag, LastTaxTags> {

    transient MapState<String,CircularFifoBuffer> lastThreeTaxTagsMapState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        lastThreeTaxTagsMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-three-tax-tags-map", String.class, CircularFifoBuffer.class));
    }

    @Override
    public void flatMap(TaxTag taxTag, Collector<LastTaxTags> collector) throws Exception {
        add(taxTag);
        String key = taxTag.getState().name();
        CircularFifoBuffer lastTaxTags = lastThreeTaxTagsMapState.get(key);
        LastTaxTags.Builder taxTagsBuilder = LastTaxTags.newBuilder().setStateName(taxTag.getState().name()).setTaxTags(new ArrayList<>());
        if(lastTaxTags != null){
            for (Object lastTaxTag : lastTaxTags) {
                taxTagsBuilder.getTaxTags().add((TaxTag) lastTaxTag);
            }
        }
        collector.collect(taxTagsBuilder.build());
    }

    public void add(TaxTag taxTag) throws Exception {

        String key = taxTag.getState().name();
        CircularFifoBuffer lastTaxTags = lastThreeTaxTagsMapState.get(key);
        if(lastTaxTags == null){
            lastTaxTags = new CircularFifoBuffer(3);
        }
        lastTaxTags.add(taxTag);
        lastThreeTaxTagsMapState.put(key,lastTaxTags);
    }

}
