package redis.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by sdhillon on 4/3/15.
 */
public class BytesKeySerializer extends Serializer<BytesKey> {
    public void write (Kryo kryo, Output output, BytesKey object) {
        final byte[] bytes = object.getBytes();
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    @Override
    public BytesKey read(Kryo kryo, Input input, Class<BytesKey> bytesKeyClass) {
        int bytesLength = input.readInt();
        return new BytesKey(input.readBytes(bytesLength));
    }

}
