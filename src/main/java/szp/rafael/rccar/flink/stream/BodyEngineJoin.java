package szp.rafael.rccar.flink.stream;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.RemoteControl;
import szp.rafael.rccar.dto.Wheel;

import java.util.List;


public class BodyEngineJoin extends KeyedCoProcessFunction<String, Body, Engine, Tuple4<Body,Engine, RemoteControl, List<Wheel>>> {

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

    private Tuple4<Body,Engine,RemoteControl,List<Wheel>> join(Body body, Engine engine) {
        return new Tuple4<Body,Engine,RemoteControl,List<Wheel>>(body, engine,null,null);
    }

    @Override
    public void processElement1(Body body, KeyedCoProcessFunction<String, Body, Engine, Tuple4<Body, Engine, RemoteControl, List<Wheel>>>.Context context, Collector<Tuple4<Body, Engine, RemoteControl, List<Wheel>>> collector) throws Exception {
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
            System.out.println("1>timer = " + procTimeState.value()+ " sku: "+body.getPart().getSku());
        }
    }

    @Override
    public void processElement2(Engine engine, KeyedCoProcessFunction<String, Body, Engine, Tuple4<Body, Engine, RemoteControl, List<Wheel>>>.Context context, Collector<Tuple4<Body, Engine, RemoteControl, List<Wheel>>> collector) throws Exception {
        var body = bodyState.value();
        if (body != null) {
            collector.collect(join(body, engine));
            System.out.println("coletou os 2" + body.toString()+ " | " + engine.toString());
            bodyState.clear();
        }else{
            engineState.update(engine);
            long timer =  context.timerService().currentProcessingTime() + TIMEOUT;
            timeState.update(timer);
            procTimeState.update( context.timerService().currentProcessingTime());
            context.timerService().registerProcessingTimeTimer(timer);
            System.out.println("2>timer = " + procTimeState.value() + " sku: "+engine.getPart().getSku());
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, Body, Engine, Tuple4<Body,Engine, RemoteControl, List<Wheel>>> .OnTimerContext ctx, Collector<Tuple4<Body,Engine, RemoteControl, List<Wheel>>>  out) throws Exception {
        if(timeState.value()!=null){
            Long proc = procTimeState.value()!=null ? procTimeState.value() : timestamp;
            System.out.println("Elapsed="+(timestamp-proc)+"ms");
            ctx.timerService().deleteProcessingTimeTimer(timeState.value());
        }
        if(bodyState.value() != null) {
            System.out.println("timeout reached, time state: "+ timeState.value() + " stamp: "+ timestamp  + " proc:"+procTimeState.value());
            System.out.println("bodyState = " + bodyState.value());
            System.out.println("engineState = " + engineState.value());
            out.collect(new Tuple4<Body,Engine,RemoteControl,List<Wheel>>(bodyState.value(), engineState.value(),null,null));
            bodyState.clear();
        }
        if(engineState.value() != null) {
            engineState.clear();
        }
    }
}
