package org.apache.cassandra.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.cassandra.io.IVersionedSerializer;

public class EchoMessage
{
    public static IVersionedSerializer<EchoMessage> serializer = new EchoMessageSerializer();

    public static class EchoMessageSerializer implements IVersionedSerializer<EchoMessage>
    {
        public void serialize(EchoMessage t, DataOutput out, int version) throws IOException
        {
        }

        public EchoMessage deserialize(DataInput in, int version) throws IOException
        {
            return new EchoMessage();
        }

        public long serializedSize(EchoMessage t, int version)
        {
            return 0;
        }
    }
}
