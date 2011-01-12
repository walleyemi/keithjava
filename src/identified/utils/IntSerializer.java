package identified.utils;

import krati.sos.*;
import it.unimi.dsi.fastutil.ints.*;
import java.nio.ByteBuffer;

public class IntSerializer implements krati.sos.ObjectSerializer<Integer> {
    public byte[] serialize(Integer i){
        ByteBuffer bytes = ByteBuffer.allocate(4);
        bytes.putInt(i.intValue());
        return bytes.array();
    }

    public Integer construct(byte[] in) {
        ByteBuffer bytes = ByteBuffer.wrap(in);
        if(bytes.array().length % 4 != 0){
            throw new ObjectConstructionException("Byte array cannot be converted to an int because length is not divisible by 4");
        }
        return Integer.valueOf(bytes.getInt());
    }
}
