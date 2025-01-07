package szp.rafael.rccar.flink.stream;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.dto.RemoteControl;

public class RCCarRemoteControlJoin extends KeyedCoProcessFunction<String, RCCar, RemoteControl,RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(RCCarRemoteControlJoin.class);
    private ValueState<RCCar> rccarState;
    private ValueState<RemoteControl> remoteControlState;
    private ValueState<Long> timeoutState;
    private final Long TIMEOUT = 5 * 1000L;

    @Override
    public void open(OpenContext openContext) throws Exception {
        var rccarDescriptor = new ValueStateDescriptor<RCCar>("rccar-state", RCCar.class);
        var remoteControlDescriptor = new ValueStateDescriptor<RemoteControl>("remote-control-state", RemoteControl.class);
        var timeoutDescriptor = new ValueStateDescriptor<Long>("timeout-state", Long.class);

        rccarState = getRuntimeContext().getState(rccarDescriptor);
        remoteControlState = getRuntimeContext().getState(remoteControlDescriptor);
        timeoutState = getRuntimeContext().getState(timeoutDescriptor);
    }

    @Override
    public void processElement1(RCCar rcCar, KeyedCoProcessFunction<String, RCCar, RemoteControl, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        var remoteControl = remoteControlState.value();
        if (remoteControl != null) {
            collector.collect(join(rcCar, remoteControl));
            remoteControlState.clear();
        } else {
            rccarState.update(rcCar);
            timeoutState.update(context.timerService().currentProcessingTime() + TIMEOUT);
            context.timerService().registerProcessingTimeTimer(timeoutState.value());
            logger.info("Waiting {} for Sku {}'s Remote control arrival.",TIMEOUT,rcCar.getSku());
        }

    }

    @Override
    public void processElement2(RemoteControl remoteControl, KeyedCoProcessFunction<String, RCCar, RemoteControl, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        remoteControlState.update(remoteControl);
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, RCCar, RemoteControl, RCCar>.OnTimerContext ctx, Collector<RCCar> out) throws Exception {
        Long timer = timeoutState.value();
        if(timer!=null){
            ctx.timerService().deleteProcessingTimeTimer(timer);
        }
        var rcCar = rccarState.value();
        var remoteControl = remoteControlState.value();
        if (rcCar != null) {
            out.collect(join(rcCar,remoteControl));
            rccarState.clear();
            remoteControlState.clear();
        }
    }

    private RCCar join(RCCar rcCar, RemoteControl remoteControl){
        rcCar.setRemoteControl(remoteControl);
        rcCar.setSituation(CarSituation.INCOMPLETE);
        if(remoteControl==null){
            rcCar.setSituation(CarSituation.MISSING_REMOTE_CONTROL);
        }
        return rcCar;
    }
}
