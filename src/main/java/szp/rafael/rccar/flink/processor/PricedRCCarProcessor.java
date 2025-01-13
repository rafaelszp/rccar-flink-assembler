package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;


public class PricedRCCarProcessor extends KeyedCoProcessFunction<String, RCCar, RCCar, RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(PricedRCCarProcessor.class);

    transient ValueState<RCCar> pricedState;
    transient ValueState<RCCar> unpricedState;
    final Long WAIT_TIMEOUT = 15 * 1000L;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ValueStateDescriptor<RCCar> pricedDescriptor = new ValueStateDescriptor<>("priced-state", RCCar.class);
        ValueStateDescriptor<RCCar> unpricedDescriptor = new ValueStateDescriptor<>("unpriced-state", RCCar.class);
        pricedState = getRuntimeContext().getState(pricedDescriptor);
        unpricedState = getRuntimeContext().getState(unpricedDescriptor);
    }

    @Override
    public void processElement1(RCCar rcCar, KeyedCoProcessFunction<String, RCCar, RCCar, RCCar>.Context context, Collector<RCCar> collector) throws Exception {

        unpricedState.update(rcCar);
        var priced = pricedState.value();
        if (priced == null) {
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + WAIT_TIMEOUT);
        }
    }

    @Override
    public void processElement2(RCCar rcCar, KeyedCoProcessFunction<String, RCCar, RCCar, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        pricedState.update(rcCar);
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, RCCar, RCCar, RCCar>.OnTimerContext ctx, Collector<RCCar> out) throws Exception {

        ctx.timerService().deleteProcessingTimeTimer(timestamp);
        var unpriced = unpricedState.value();
        var priced = pricedState.value();

        RCCar collected = null;
        if (priced != null) {
            collected = RCCar.newBuilder(priced).setSituation(CarSituation.COMPLETE).build();
        } else {
            collected = RCCar.newBuilder(unpriced).setSituation(CarSituation.AWAITING_PRICE).build();
        }
        out.collect(collected);
        logger.info("Car {} is {}", collected.getSku(), collected.getSituation());

        unpricedState.clear();
        pricedState.clear();
    }
}
