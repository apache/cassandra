package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

/**
 * breaks up execution space
 * why are instances vetoed?
 */
public class EpochInstance extends AbstractTokenInstance
{
    private final long epoch;
    private volatile boolean vetoed;

    public EpochInstance(InetAddress leader, Token token, UUID cfId, long epoch, Scope type)
    {
        super(leader, cfId, token, type);
        this.epoch = epoch;
    }

    public EpochInstance(UUID id, InetAddress leader, Token token, UUID cfId, long epoch, Scope type)
    {
        super(id, leader, cfId, token, type);
        this.epoch = epoch;
    }

    public EpochInstance(EpochInstance i)
    {
        super(i);
        this.epoch = i.epoch;
        this.vetoed = i.vetoed;
    }

    @Override
    public Instance copy()
    {
        return new EpochInstance(this);
    }

    @Override
    public Instance copyRemote()
    {
        Instance instance = new EpochInstance(this.id, this.leader, this.token, this.cfId, this.epoch, this.scope);
        instance.ballot = ballot;
        instance.noop = noop;
        instance.state = state;
        instance.dependencies = dependencies;
        return instance;
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

    public long getEpoch()
    {
        return epoch;
    }

    @Override
    public Type getType()
    {
        return Type.EPOCH;
    }

    @Override
    public boolean getLeaderAttrsMatch()
    {
        return super.getLeaderAttrsMatch() && !isVetoed();
    }

    public boolean isVetoed()
    {
        return vetoed;
    }

    public void setVetoed(boolean vetoed)
    {
        this.vetoed = vetoed;
    }

    @Override
    public boolean skipExecution()
    {
        return super.skipExecution() || vetoed;
    }

    @Override
    public void applyRemote(Instance remote)
    {
        assert remote instanceof EpochInstance;
        super.applyRemote(remote);
        this.vetoed = ((EpochInstance) remote).vetoed;
    }

    @Override
    protected String toStringExtra()
    {
        return ", epoch=" + epoch + ", vetoed=" + vetoed;
    }

    private static final IVersionedSerializer<EpochInstance> commonSerializer = new IVersionedSerializer<EpochInstance>()
    {
        @Override
        public void serialize(EpochInstance instance, DataOutputPlus out, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            UUIDSerializer.serializer.serialize(instance.getId(), out, major);
            CompactEndpointSerializationHelper.serialize(instance.getLeader(), out);
            Token.serializer.serialize(instance.token, out, major);
            UUIDSerializer.serializer.serialize(instance.cfId, out, major);
            out.writeLong(instance.epoch);
            out.writeInt(instance.scope.ordinal());
            out.writeBoolean(instance.vetoed);
        }

        @Override
        public EpochInstance deserialize(DataInput in, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            EpochInstance instance = new EpochInstance(UUIDSerializer.serializer.deserialize(in, major),
                                                       CompactEndpointSerializationHelper.deserialize(in),
                                                       Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major),
                                                       UUIDSerializer.serializer.deserialize(in, major),
                                                       in.readLong(),
                                                       Scope.values()[in.readInt()]);

            instance.vetoed = in.readBoolean();
            return instance;
        }

        @Override
        public long serializedSize(EpochInstance instance, int version)
        {
            int major = EpaxosService.Version.major(version);
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.getId(), major);
            size += CompactEndpointSerializationHelper.serializedSize(instance.getLeader());
            size += Token.serializer.serializedSize(instance.token, major);
            size += UUIDSerializer.serializer.serializedSize(instance.cfId, major);
            size += 8;
            size += 4;
            size += 1;
            return size;
        }
    };

    public static final IVersionedSerializer<Instance> serializer = new IVersionedSerializer<Instance>()
    {
        private final ExternalSerializer baseSerializer = new ExternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof EpochInstance;
            commonSerializer.serialize((EpochInstance) instance, out, version);
            baseSerializer.serialize(instance, out, version);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = commonSerializer.deserialize(in, version);
            baseSerializer.deserialize(instance, in, version);
            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            assert instance instanceof EpochInstance;
            return commonSerializer.serializedSize((EpochInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };

    public static final IVersionedSerializer<Instance> internalSerializer = new IVersionedSerializer<Instance>()
    {
        private final InternalSerializer baseSerializer = new InternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof EpochInstance;
            commonSerializer.serialize((EpochInstance) instance, out, version);
            baseSerializer.serialize(instance, out, version);
        }

        @Override
        public Instance deserialize(DataInput in, int version) throws IOException
        {
            Instance instance = commonSerializer.deserialize(in, version);
            baseSerializer.deserialize(instance, in, version);
            return instance;
        }

        @Override
        public long serializedSize(Instance instance, int version)
        {
            assert instance instanceof EpochInstance;
            return commonSerializer.serializedSize((EpochInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };
}
