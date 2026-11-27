package dev.rainsolo.minirpc.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Zero-dependency default serializer; can be replaced via SPI (Hessian, Protobuf, JSON...). */
public final class JdkSerializer implements Serializer {

    @Override
    public byte code() {
        return 1;
    }

    @Override
    public byte[] serialize(Object object) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
        try (ObjectOutputStream stream = new ObjectOutputStream(buffer)) {
            stream.writeObject(object);
        }
        return buffer.toByteArray();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T deserialize(byte[] data, Class<T> type) throws IOException, ClassNotFoundException {
        try (ObjectInputStream stream = new ObjectInputStream(new ByteArrayInputStream(data))) {
            return (T) stream.readObject();
        }
    }
}
