package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

/**
 * Sends information used for failure detection
 * along with epaxos messages
 */
public class MessageEnvelope<T> implements IEpochMessage
{
    public final Token token;
    public final UUID cfId;
    public final long epoch;
    public final Scope scope;
    public final T contents;

    public MessageEnvelope(Token token, UUID cfId, long epoch, Scope scope, T contents)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
        this.scope = scope;
        this.contents = contents;
    }

    @Override
    public Token getToken()
    {
        return token;
    }

    @Override
    public UUID getCfId()
    {
        return cfId;
    }

    @Override
    public long getEpoch()
    {
        return epoch;
    }

    @Override
    public Scope getScope()
    {
        return scope;
    }

    public static <T> Serializer<T> getSerializer(IVersionedSerializer<T> payloadSerializer)
    {
        return new Serializer<>(payloadSerializer);
    }

    @Override
    public String toString()
    {
        return "MessageEnvelope{" +
               "contents=" + contents +
               '}';
    }

    private static class Serializer<T> implements IVersionedSerializer<MessageEnvelope<T>>
    {
        private final IVersionedSerializer<T> payloadSerializer;

        private Serializer(IVersionedSerializer<T> payloadSerializer)
        {
            this.payloadSerializer = payloadSerializer;
        }

        @Override
        public void serialize(MessageEnvelope<T> envelope, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(envelope.token, out, version);
            UUIDSerializer.serializer.serialize(envelope.cfId, out, version);
            out.writeLong(envelope.epoch);
            Scope.serializer.serialize(envelope.scope, out, version);

            out.writeBoolean(envelope.contents != null);
            if (envelope.contents != null)
            {
                payloadSerializer.serialize(envelope.contents, out, version);
            }
        }

        @Override
        public MessageEnvelope<T> deserialize(DataInput in, int version) throws IOException
        {
            return new MessageEnvelope<>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version),
                                         UUIDSerializer.serializer.deserialize(in, version),
                                         in.readLong(),
                                         Scope.serializer.deserialize(in, version),
                                         in.readBoolean() ? payloadSerializer.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(MessageEnvelope<T> envelope, int version)
        {
            long size = Token.serializer.serializedSize(envelope.token, version);
            size += UUIDSerializer.serializer.serializedSize(envelope.cfId, version);
            size += 8;  // envelope.epoch
            size += Scope.serializer.serializedSize(envelope.scope, version);

            size += 1;
            if (envelope.contents != null)
            {
                size += payloadSerializer.serializedSize(envelope.contents, version);
            }
            return size;
        }
    }
}
