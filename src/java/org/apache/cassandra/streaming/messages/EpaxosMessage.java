package org.apache.cassandra.streaming.messages;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.service.epaxos.InstanceStreamReader;
import org.apache.cassandra.service.epaxos.InstanceStreamWriter;
import org.apache.cassandra.service.epaxos.Scope;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.UUID;

public class EpaxosMessage extends StreamMessage
{
    public final UUID taskId;
    public final UUID cfId;
    public final Range<Token> range;
    public final Scope scope;

    public EpaxosMessage(UUID taskId, UUID cfId, Range<Token> range, Scope scope)
    {
        super(Type.EPAXOS);
        this.taskId = taskId;
        this.cfId = cfId;
        this.range = range;
        this.scope = scope;
    }

    public static Serializer<EpaxosMessage> serializer = new Serializer<EpaxosMessage>()
    {
        @Override
        public EpaxosMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInputStream input = new DataInputStream(Channels.newInputStream(in));
            EpaxosMessage message = new EpaxosMessage(UUIDSerializer.serializer.deserialize(input, version),
                                                      UUIDSerializer.serializer.deserialize(input, version),
                                                      new Range<>(Token.serializer.deserialize(input, DatabaseDescriptor.getPartitioner(), version),
                                                                  Token.serializer.deserialize(input, DatabaseDescriptor.getPartitioner(), version)),
                                                      Scope.serializer.deserialize(input, version));

            InstanceStreamReader reader = new InstanceStreamReader(message.cfId, message.range, message.scope, session.peer);
            reader.read(in, session);

            return message;
        }

        @Override
        public void serialize(EpaxosMessage message, DataOutputStreamPlus out, int version, StreamSession session) throws IOException
        {
            UUIDSerializer.serializer.serialize(message.taskId, out, version);
            UUIDSerializer.serializer.serialize(message.cfId, out, version);
            Token.serializer.serialize(message.range.left, out, version);
            Token.serializer.serialize(message.range.right, out, version);
            Scope.serializer.serialize(message.scope, out, version);

            InstanceStreamWriter writer = new InstanceStreamWriter(message.cfId, message.range, message.scope, session.peer);
            writer.write(out);

            session.epaxosTransferComplete(message.taskId);
        }
    };
}
