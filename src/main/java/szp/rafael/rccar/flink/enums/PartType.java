package szp.rafael.rccar.flink.enums;

public enum PartType {
    BODY,
    WHEEL,
    REMOTE_CONTROL,
    ENGINE;

    public static String name(PartType partType){
        return partType.name().toLowerCase().replaceAll("_", "-");
    }
}
