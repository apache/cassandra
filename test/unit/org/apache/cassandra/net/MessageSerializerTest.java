package org.apache.cassandra.net;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class MessageSerializerTest extends SchemaLoader
{
    @Test
    public void testDeserialize() throws IOException
    {
        String wire = "ca552dfa0000010000000080000131047f00000100000000000000000000004800094b657973706163653100046b65793100000001000003e801000003e8800000008000000000000000000000010007436f6c756d6e310000000000000000000000000461736466000000000000000000000000000000000000000000000000000000000000000000000000";
        byte[] bytes = FBUtilities.hexToBytes(wire);
        check(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

    private void check(DataInputStream in) throws IOException
    {
        IncomingTcpConnection.readHeader(in);
        IncomingTcpConnection.readMessage(in);
    }

    @Test
    public void testRoundTrip() throws IOException
    {
        RowMutation rm;
        rm = new RowMutation("Keyspace1", ByteBufferUtil.bytes("key1"));
        rm.add(new QueryPath("Standard1", null, ByteBufferUtil.bytes("Column1")), ByteBufferUtil.bytes("asdf"), 0);
        Message message = rm.makeRowMutationMessage();
        ByteBuffer bb = MessagingService.serialize(message);
        check(new DataInputStream(new ByteArrayInputStream(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining())));
    }
}
