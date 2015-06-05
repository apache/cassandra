package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

/**
 * Message which contains epoch information
 */
public abstract class AbstractEpochMessage implements IEpochMessage
{
    public final Token token;
    public final UUID cfId;
    public final long epoch;
    public final Scope scope;

    public AbstractEpochMessage(Token token, UUID cfId, long epoch, Scope scope)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
        this.scope = scope;
    }

    protected AbstractEpochMessage(AbstractEpochMessage epochInfo)
    {
        this.token = epochInfo.token;
        this.cfId = epochInfo.cfId;
        this.epoch = epochInfo.epoch;
        this.scope = epochInfo.scope;
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

    private static class EpochInfo extends AbstractEpochMessage
    {
        public EpochInfo(Token token, UUID cfId, long epoch, Scope scope)
        {
            super(token, cfId, epoch, scope);
        }
    }

    protected static IVersionedSerializer<AbstractEpochMessage> serializer = new IVersionedSerializer<AbstractEpochMessage>()
    {
        @Override
        public void serialize(AbstractEpochMessage msg, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(msg.token, out, EpaxosService.Version.major(version));
            UUIDSerializer.serializer.serialize(msg.cfId, out, version);
            out.writeLong(msg.epoch);
            Scope.serializer.serialize(msg.scope, out, version);
        }

        @Override
        public AbstractEpochMessage deserialize(DataInput in, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            return new EpochInfo(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major),
                                 UUIDSerializer.serializer.deserialize(in, major),
                                 in.readLong(),
                                 Scope.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(AbstractEpochMessage msg, int version)
        {
            int major = EpaxosService.Version.major(version);
            long size = Token.serializer.serializedSize(msg.token, major);
            size += UUIDSerializer.serializer.serializedSize(msg.cfId, major);
            size += 8;  // response.epoch
            size += Scope.serializer.serializedSize(msg.scope, version);
            return size;
        }
    };
}
