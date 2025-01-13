package szp.rafael.rccar.flink.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import szp.rafael.rccar.dto.CarSituation;
import szp.rafael.rccar.dto.RCCar;
import szp.rafael.rccar.flink.enums.PartType;

public class RCcarFlatMap extends RichCoFlatMapFunction<RCCar, RCCar, RCCar> {

    transient ValueState<RCCar> carState1;
    transient ValueState<RCCar> carState2;
    PartType element2PartyType;

    public RCcarFlatMap(PartType element2PartyType) {
        this.element2PartyType = element2PartyType;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        carState1 = getRuntimeContext().getState(new ValueStateDescriptor<>("carState1", RCCar.class));
        carState2 = getRuntimeContext().getState(new ValueStateDescriptor<>("carState2", RCCar.class));
    }

    @Override
    public void flatMap1(RCCar rcCar, Collector<RCCar> collector) throws Exception {
        carState1.update(rcCar);
        if(carState2.value()!=null){
            collector.collect(join(rcCar, carState2.value()));
        }
    }

    @Override
    public void flatMap2(RCCar rcCar, Collector<RCCar> collector) throws Exception {
        carState2.update(rcCar);
        if(carState1.value()!=null){
            collector.collect(join(carState1.value(), rcCar));
        }
    }

    public RCCar join(RCCar rcCar1, RCCar rcCar2) {
        switch (element2PartyType){
            case BODY -> {
                if (rcCar2.getBody() == null) {
                    return RCCar.newBuilder(rcCar2).setSituation(CarSituation.MISSING_BODY).build();
                }
                return RCCar.newBuilder(rcCar1).setBody(rcCar2.getBody()).setSituation(CarSituation.COMPLETE).build();
            }
            case ENGINE -> {
                if (rcCar2.getEngine() == null) {
                    return RCCar.newBuilder(rcCar2).setSituation(CarSituation.MISSING_ENGINE).build();
                }
                return RCCar.newBuilder(rcCar1).setEngine(rcCar2.getEngine()).setSituation(CarSituation.COMPLETE).build();
            }
            case REMOTE_CONTROL -> {
                if (rcCar2.getRemoteControl() == null) {
                    return RCCar.newBuilder(rcCar2).setSituation(CarSituation.MISSING_REMOTE_CONTROL).build();
                }
                return RCCar.newBuilder(rcCar1).setRemoteControl(rcCar2.getRemoteControl()).setSituation(CarSituation.COMPLETE).build();
            }
            case WHEEL -> {
                if (rcCar2.getWheels().isEmpty()) {
                    return RCCar.newBuilder(rcCar2).setSituation(CarSituation.MISSING_WHEELS).build();
                }
                return RCCar.newBuilder(rcCar1).setWheels(rcCar2.getWheels()).setSituation(CarSituation.COMPLETE).build();
            }
        }
        return RCCar.newBuilder(rcCar2).setSituation(CarSituation.INCOMPLETE).build();
    }


}
