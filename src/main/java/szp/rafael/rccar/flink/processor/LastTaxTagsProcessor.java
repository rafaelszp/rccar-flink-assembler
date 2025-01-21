package szp.rafael.rccar.flink.processor;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import szp.rafael.rccar.dto.LastTaxTags;
import szp.rafael.rccar.dto.TaxTag;

import java.io.IOException;
import java.util.ArrayList;

public class LastTaxTagsProcessor extends RichFlatMapFunction<TaxTag, LastTaxTags> {

    transient MapState<String,LastTaxTags> lastTaxTagsMapState;
    transient MapState<String,CircularFifoBuffer> lastThreeTaxTagsMapState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        lastTaxTagsMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-tax-tags-map", String.class, LastTaxTags.class));
        lastThreeTaxTagsMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-three-tax-tags-map", String.class, CircularFifoBuffer.class));
    }


    @Override
    public void flatMap(TaxTag taxTag, Collector<LastTaxTags> collector) throws Exception {
        add(taxTag);
        updateList();
        LastTaxTags.Builder taxTagsBuilder = LastTaxTags.newBuilder().setStateName(taxTag.getState().name()).setTaxTags(new ArrayList<>());
        taxTagListState.get().forEach(tag -> taxTagsBuilder.getTaxTags().add(tag));
        collector.collect(taxTagsBuilder.build());
    }

    public void add(TaxTag taxTag) throws IOException {
        CircularFifoBuffer lastTaxTags = lastTaxTagsState.value();
        if (lastTaxTags == null) {
            lastTaxTags = new CircularFifoBuffer(3);
        }
        lastTaxTags.add(taxTag);
        lastTaxTagsState.update(lastTaxTags);
    }

    public void updateList() throws Exception {
        taxTagListState.clear();
        CircularFifoBuffer lastTaxTags = lastTaxTagsState.value();
        for (Object taxTag : lastTaxTags) {
            taxTagListState.add((TaxTag) taxTag);
        }
    }

}
