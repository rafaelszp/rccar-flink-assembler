package szp.rafael.rccar.flink.processor;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import szp.rafael.rccar.dto.Body;
import szp.rafael.rccar.dto.Engine;
import szp.rafael.rccar.dto.Part;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.dto.RemoteControl;
import szp.rafael.rccar.dto.Wheel;
import szp.rafael.rccar.flink.enums.PartType;

import java.util.ArrayList;

public class PartPricedRCCarJoin extends KeyedCoProcessFunction<String, Part, RCCar, RCCar> {

    ValueState<Part> partState;
    ValueState<RCCar> rcCarState;

    @Override
    public void processElement1(Part part, KeyedCoProcessFunction<String, Part, RCCar, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        RCCar rcCar = rcCarState.value();
        if (rcCar != null) {
            var car = RCCar.newBuilder(rcCar);
            if (PartType.BODY.name().equals(part.getPartName())) {
                car.setBody(Body.newBuilder(rcCar.getBody()).setPart(part).build());
            }
            if (PartType.ENGINE.name().equals(part.getPartName())) {
                car.setEngine(Engine.newBuilder(rcCar.getEngine()).setPart(part).build());
            }
            if (PartType.REMOTE_CONTROL.name().equals(part.getPartName())) {
                car.setRemoteControl(RemoteControl.newBuilder(rcCar.getRemoteControl()).setPart(part).build());
            }
            if (PartType.WHEEL.name().equals(part.getPartName())) {
                var wheels = new ArrayList<Wheel>();
                for (Wheel wheel : rcCar.getWheels()) {
                    wheel.getPart().setPrice(part.getPrice());
                    wheels.add(wheel);
                }
                car.setWheels(wheels);
            }
            collector.collect(rcCar);
            rcCarState.clear();
        } else {
            partState.update(part);
        }
    }

    @Override
    public void processElement2(RCCar rcCar, KeyedCoProcessFunction<String, Part, RCCar, RCCar>.Context context, Collector<RCCar> collector) throws Exception {
        var part = partState.value();
        if(part!=null){
            var car = RCCar.newBuilder(rcCar);
            if (PartType.BODY.name().equals(part.getPartName())) {
                car.setBody(Body.newBuilder(rcCar.getBody()).setPart(part).build());
            }
            if (PartType.ENGINE.name().equals(part.getPartName())) {
                car.setEngine(Engine.newBuilder(rcCar.getEngine()).setPart(part).build());
            }
            if (PartType.REMOTE_CONTROL.name().equals(part.getPartName())) {
                car.setRemoteControl(RemoteControl.newBuilder(rcCar.getRemoteControl()).setPart(part).build());
            }
            if (PartType.WHEEL.name().equals(part.getPartName())) {
                var wheels = new ArrayList<Wheel>();
                for (Wheel wheel : rcCar.getWheels()) {
                    wheel.getPart().setPrice(part.getPrice());
                    wheels.add(wheel);
                }
                car.setWheels(wheels);
            }
            collector.collect(rcCar);
            partState.clear();
        } else {
            rcCarState.update(rcCar);
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        ValueStateDescriptor<Part> partDescriptor = new ValueStateDescriptor<>("part-state", Part.class);
        ValueStateDescriptor<RCCar> rcCarDescriptor = new ValueStateDescriptor<>("rccar-state", RCCar.class);

        partState = getRuntimeContext().getState(partDescriptor);
        rcCarState = getRuntimeContext().getState(rcCarDescriptor);
    }
}
