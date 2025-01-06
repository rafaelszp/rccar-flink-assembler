package szp.rafael.rccar.flink.deserializers;

import szp.rafael.rccar.dto.Body;

public class BodyDeserializer extends AbstractDeserializer<Body> {
    public BodyDeserializer() {
        super(Body.class);
    }
}
