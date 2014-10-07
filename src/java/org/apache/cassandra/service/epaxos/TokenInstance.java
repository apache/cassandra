package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Sets;

/**
 * Adds new tokens to the token state manager
 */
public class TokenInstance extends AbstractTokenInstance
{
    // the token range this instance is splitting
    // this is important because the range this instance
    // end up splitting may be different than the range
    // that exists when it's created
    private volatile Range<Token> splitRange;
    private volatile boolean leaderRangeMatched = true;

    public TokenInstance(InetAddress leader, UUID cfId, Token token, Range<Token> splitRange, Scope type)
    {
        super(leader, cfId, token, type);
        this.splitRange = splitRange;
    }

    public TokenInstance(UUID id, InetAddress leader, UUID cfId, Token token, Range<Token> splitRange, Scope type)
    {
        super(id, leader, cfId, token, type);
        this.splitRange = splitRange;
    }

    public TokenInstance(TokenInstance i)
    {
        super(i);
        this.splitRange = i.splitRange;
    }

    public Range<Token> getSplitRange()
    {
        return splitRange;
    }

    public void setSplitRange(Range<Token> splitRange)
    {
        this.splitRange = splitRange;
    }

    static Range<Token> mergeRanges(Range<Token> r1, Range<Token> r2)
    {
        if (!r1.intersects(r2))
        {
            throw new AssertionError(String.format("Ranges %s and %s do not intersect", r1, r2));
        }

        if (r1.equals(r2))
        {
            return r1;
        }

        boolean rewrap = r1.unwrap().size() > 1 || r2.unwrap().size() > 1;

        List<Range<Token>> normalized = Range.normalize(Sets.newHashSet(r1, r2));

        if (rewrap)
        {
            assert normalized.size() == 2;
            Range<Token> rangeRight = normalized.get(0);
            Range<Token> rangeLeft = normalized.get(1);

            Token minToken = DatabaseDescriptor.getPartitioner().getMinimumToken();
            assert rangeLeft.right.equals(minToken);
            assert rangeRight.left.equals(minToken);
            return new Range<>(rangeLeft.left, rangeRight.right);
        }
        else
        {
            assert normalized.size() == 1;
            return normalized.get(0);
        }
    }

    /**
     * used during preaccept
     */
    public void mergeLocalSplitRange(Range<Token> range)
    {
        leaderRangeMatched = range.equals(splitRange);
        splitRange = mergeRanges(splitRange, range);
    }

    @Override
    public boolean getLeaderAttrsMatch()
    {
        return super.getLeaderAttrsMatch() && leaderRangeMatched;
    }

    @Override
    public Instance copy()
    {
        return new TokenInstance(this);
    }

    @Override
    public Instance copyRemote()
    {
        return copy();
    }

    @Override
    public Type getType()
    {
        return Type.TOKEN;
    }

    private static final IVersionedSerializer<TokenInstance> commonSerializer = new IVersionedSerializer<TokenInstance>()
    {
        @Override
        public void serialize(TokenInstance instance, DataOutputPlus out, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            UUIDSerializer.serializer.serialize(instance.getId(), out, major);
            CompactEndpointSerializationHelper.serialize(instance.getLeader(), out);
            UUIDSerializer.serializer.serialize(instance.cfId, out, major);
            Token.serializer.serialize(instance.token, out, major);
            Token.serializer.serialize(instance.splitRange.left, out, major);
            Token.serializer.serialize(instance.splitRange.right, out, major);
            out.writeInt(instance.scope.ordinal());
        }

        @Override
        public TokenInstance deserialize(DataInput in, int version) throws IOException
        {
            int major = EpaxosService.Version.major(version);
            TokenInstance instance = new TokenInstance(UUIDSerializer.serializer.deserialize(in, major),
                                                       CompactEndpointSerializationHelper.deserialize(in),
                                                       UUIDSerializer.serializer.deserialize(in, major),
                                                       Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major),
                                                       new Range<>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major),
                                                                   Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), major)),
                                                       Scope.values()[in.readInt()]);

            return instance;
        }

        @Override
        public long serializedSize(TokenInstance instance, int version)
        {
            int major = EpaxosService.Version.major(version);
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.getId(), major);
            size += CompactEndpointSerializationHelper.serializedSize(instance.getLeader());
            size += UUIDSerializer.serializer.serializedSize(instance.cfId, major);
            size += Token.serializer.serializedSize(instance.token, major);
            size += Token.serializer.serializedSize(instance.splitRange.left, major);
            size += Token.serializer.serializedSize(instance.splitRange.right, major);
            size += 4;
            return size;
        }
    };

    public static final IVersionedSerializer<Instance> serializer = new IVersionedSerializer<Instance>()
    {
        private final ExternalSerializer baseSerializer = new ExternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof TokenInstance;
            commonSerializer.serialize((TokenInstance) instance, out, version);
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
            assert instance instanceof TokenInstance;
            return commonSerializer.serializedSize((TokenInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };

    public static final IVersionedSerializer<Instance> internalSerializer = new IVersionedSerializer<Instance>()
    {
        private final InternalSerializer baseSerializer = new InternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof TokenInstance;
            commonSerializer.serialize((TokenInstance) instance, out, version);
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
            assert instance instanceof TokenInstance;
            return commonSerializer.serializedSize((TokenInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };
}
