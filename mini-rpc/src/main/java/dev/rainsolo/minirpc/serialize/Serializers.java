package dev.rainsolo.minirpc.serialize;

import java.util.ServiceLoader;

/** Resolves the serializer once: SPI providers win, otherwise fall back to the JDK one. */
public final class Serializers {

    private static final Serializer INSTANCE = resolve();

    private Serializers() {
    }

    private static Serializer resolve() {
        for (Serializer serializer : ServiceLoader.load(Serializer.class)) {
            return serializer;
        }
        return new JdkSerializer();
    }

    public static Serializer defaultSerializer() {
        return INSTANCE;
    }
}
