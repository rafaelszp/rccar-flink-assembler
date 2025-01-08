package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.dto.Wheel;

import java.util.ArrayList;

public class RCCarWheelsJoin extends KeyedCoProcessFunction<String, RCCar, Wheel, RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(RCCarWheelsJoin.class);

    private ValueState<RCCar> rccarState;
    private ValueState<Integer> wheelCountState;
    private ListState<Wheel> wheelListState;
    private ValueState<Long> timeoutState;
    private final Long TIMEOUT = 5 * 1000L;

    @Override
    public void open(OpenContext openContext) throws Exception {
        var rccarDescriptor = new ValueStateDescriptor<RCCar>("rccar-state", RCCar.class);
        var wheelCountDescriptor = new ValueStateDescriptor<Integer>("wheel-count-state", Integer.class);
        var timeoutDescriptor = new ValueStateDescriptor<Long>("timeout-state", Long.class);
        var wheelsDescriptor = new ListStateDescriptor<Wheel>("wheels-state", TypeInformation.of(new TypeHint<>() {
        }));

        rccarState = getRuntimeContext().getState(rccarDescriptor);
        timeoutState = getRuntimeContext().getState(timeoutDescriptor);
        wheelCountState = getRuntimeContext().getState(wheelCountDescriptor);
        wheelListState = getRuntimeContext().getListState(wheelsDescriptor);

    }

    @Override
    public void processElement1(RCCar rcCar, KeyedCoProcessFunction<String, RCCar, Wheel, RCCar>.Context context, Collector<RCCar> collector) throws Exception {

        Integer count = wheelCountState.value()!=null?wheelCountState.value():0;
        if (count == 4) {
            collector.collect(join(rcCar, wheelListState.get()));
            wheelCountState.clear();
            wheelCountState.clear();
        } else {
            rccarState.update(rcCar);
            timeoutState.update(context.timerService().currentProcessingTime() + TIMEOUT);
            context.timerService().registerProcessingTimeTimer(timeoutState.value());
            logger.info("Waiting {} All Sku {}'s Wheels arrival. Current count: {}",TIMEOUT,rcCar.getSku(),count);
        }

    }

    @Override
    public void processElement2(Wheel wheel, KeyedCoProcessFunction<String, RCCar, Wheel, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        Integer count = wheelCountState.value();
        if(count==null || count.equals(0)){
            wheelListState.update(new ArrayList<>());
            wheelCountState.update(0);
            count = 0;
        }
        wheelListState.add(wheel);
        wheelCountState.update(wheelCountState.value() + 1);
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, RCCar, Wheel, RCCar>.OnTimerContext ctx, Collector<RCCar> out) throws Exception {
        Long timer = timeoutState.value();
        if (timer != null) {
            ctx.timerService().deleteProcessingTimeTimer(timer);
        }
        var rcCar = rccarState.value();
        var wheelsIterator = wheelListState.get();
        if (rcCar != null) {
            out.collect(join(rcCar, wheelsIterator));
            rccarState.clear();
            wheelCountState.clear();
            wheelCountState.clear();
        }
    }

    private RCCar join(RCCar rcCar, Iterable<Wheel> wheels) {
        rcCar.setWheels(new ArrayList<>());
        wheels.forEach(wheel->rcCar.getWheels().add(wheel));
        if(rcCar.getRemoteControl()!=null && rcCar.getEngine()!=null) {
            rcCar.setSituation(CarSituation.COMPLETE);
        }
        if (rcCar.getWheels().isEmpty()) {
            rcCar.setSituation(CarSituation.MISSING_WHEELS);
        }
        return rcCar;
    }
}
