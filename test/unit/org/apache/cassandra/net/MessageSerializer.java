package org.apache.cassandra.net;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.cassandra.io.ICompactSerializer;

public class MessageSerializer implements ICompactSerializer<Message>
{
    public void serialize(Message t, DataOutputStream dos, int version) throws IOException
    {
        assert t.getVersion() == version : "internode protocol version mismatch"; // indicates programmer error.
        Header.serializer().serialize( t.header_, dos, version);
        byte[] bytes = t.getMessageBody();
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    public Message deserialize(DataInputStream dis, int version) throws IOException
    {
        Header header = Header.serializer().deserialize(dis, version);
        int size = dis.readInt();
        byte[] bytes = new byte[size];
        dis.readFully(bytes);
        return new Message(header, bytes, version);
    }
}
