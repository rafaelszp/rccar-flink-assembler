package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.RCCar;


public class BodyEngineJoin extends KeyedCoProcessFunction<String, Body, Engine, RCCar> {

    private static final Logger logger = LoggerFactory.getLogger(RCCarRemoteControlJoin.class);

    private ValueState<Body> bodyState;
    private ValueState<Engine> engineState;
    private ValueState<Long> timeState;
    private ValueState<Long> procTimeState;

    private final long TIMEOUT = 5 * 1000;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        var engineStateDescriptor = new ValueStateDescriptor<Engine>("engine-state", Engine.class);
        var bodyStateDescriptor = new ValueStateDescriptor<Body>("body-state", Body.class);
        var timeStateDescriptor = new ValueStateDescriptor<Long>("time-state", Long.class);
        var procTimeStateDescriptor = new ValueStateDescriptor<Long>("time-state", Long.class);
        bodyState = getRuntimeContext().getState(bodyStateDescriptor);
        engineState = getRuntimeContext().getState(engineStateDescriptor);
        timeState = getRuntimeContext().getState(timeStateDescriptor);
        procTimeState = getRuntimeContext().getState(procTimeStateDescriptor);
    }

    private RCCar join(Body body, Engine engine){

        var builder = RCCar.newBuilder().setBody(body).setEngine(engine);
        if(body!=null){
            builder.setSku(body.getPart().getSku());
        }else if(engine!=null){
            builder.setSku(engine.getPart().getSku());
        }
        if(body==null){
            builder.setSituation(CarSituation.MISSING_BODY);
        }
        if(engine==null){
            builder.setSituation(CarSituation.MISSING_ENGINE);
        }
        return builder.clearWheels().clearRemoteControl().build();
    }

    @Override
    public void processElement1(Body body, KeyedCoProcessFunction<String, Body, Engine, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        var engine = engineState.value();
        if (engine != null) {
            collector.collect(join(body, engine));
            engineState.clear();
        } else {
            bodyState.update(body);
            long timer = context.timerService().currentProcessingTime() + TIMEOUT;
            timeState.update(timer);
            procTimeState.update( context.timerService().currentProcessingTime());
            context.timerService().registerProcessingTimeTimer(timer);
            logger.info("Waiting {} Sku {}'s Engine arrival.",TIMEOUT,body.getPart().getSku());
        }
    }

    @Override
    public void processElement2(Engine engine, KeyedCoProcessFunction<String, Body, Engine, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        var body = bodyState.value();
        if (body != null) {
            collector.collect(join(body, engine));
            bodyState.clear();
        }else{
            engineState.update(engine);
            long timer =  context.timerService().currentProcessingTime() + TIMEOUT;
            timeState.update(timer);
            procTimeState.update( context.timerService().currentProcessingTime());
            context.timerService().registerProcessingTimeTimer(timer);
            logger.info("Waiting {} Sku {} Body arrival.",TIMEOUT,body.getPart().getSku());
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, Body, Engine, RCCar> .OnTimerContext ctx, Collector<RCCar>  out) throws Exception {
        if(timeState.value()!=null){
            ctx.timerService().deleteProcessingTimeTimer(timeState.value());
        }
        if(bodyState.value() != null || engineState.value() != null) {
            out.collect(join(bodyState.value(), engineState.value()));
            bodyState.clear();
            engineState.clear();
        }
    }
}
