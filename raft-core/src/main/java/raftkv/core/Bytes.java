package raftkv.core;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Immutable byte string with value equality. {@link #toString()} omits content to keep user data out of logs. */
public final class Bytes {

    private final byte[] data;

    private Bytes(byte[] data) {
        this.data = data;
    }

    public static Bytes copyOf(byte[] source) {
        return new Bytes(source.clone());
    }

    public static Bytes ofUtf8(String text) {
        return new Bytes(text.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] toByteArray() {
        return data.clone();
    }

    public int size() {
        return data.length;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Bytes that && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Bytes[len=" + data.length + ",hash=" + Integer.toHexString(hashCode()) + "]";
    }
}
