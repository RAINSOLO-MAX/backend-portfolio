package dev.rainsolo.minirpc.serialize;

import java.io.IOException;

/** Pluggable serializer. Implementations are discovered through JDK ServiceLoader (SPI). */
public interface Serializer {

    byte code();

    byte[] serialize(Object object) throws IOException;

    <T> T deserialize(byte[] data, Class<T> type) throws IOException, ClassNotFoundException;
}
