package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.CompactEndpointSerializationHelper;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class QueryInstance extends Instance
{
    private final SerializedRequest query;

    public QueryInstance(QueryInstance i)
    {
        super(i);
        query = i.query;
    }

    public QueryInstance(SerializedRequest query, InetAddress leader)
    {
        super(leader);
        this.query = query;
    }

    public QueryInstance(UUID id, SerializedRequest query, InetAddress leader)
    {
        super(id, leader);
        this.query = query;
    }

    public SerializedRequest getQuery()
    {
        return query;
    }

    /**
     * Returns an exact copy of this instance for internal use
     */
    public Instance copy()
    {
        return new QueryInstance(this);
    }

    public Instance copyRemote()
    {
        Instance instance = new QueryInstance(this.id, this.query, this.leader);
        instance.ballot = ballot;
        instance.noop = noop;
        instance.state = state;
        instance.dependencies = dependencies;
        return instance;
    }

    @Override
    public Type getType()
    {
        return Type.QUERY;
    }

    @Override
    public Token getToken()
    {
        return DatabaseDescriptor.getPartitioner().getToken(query.getKey());
    }

    @Override
    public UUID getCfId()
    {
        return query.getCfKey().cfId;
    }

    @Override
    public Scope getScope()
    {
        return Scope.get(getConsistencyLevel());
    }

    @Override
    public ConsistencyLevel getConsistencyLevel()
    {
        return query.getConsistencyLevel();
    }

    private static final IVersionedSerializer<QueryInstance> commonSerializer = new IVersionedSerializer<QueryInstance>()
    {
        @Override
        public void serialize(QueryInstance instance, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(instance.id, out, version);
            SerializedRequest.serializer.serialize(instance.getQuery(), out, version);
            CompactEndpointSerializationHelper.serialize(instance.leader, out);
        }

        @Override
        public QueryInstance deserialize(DataInput in, int version) throws IOException
        {
            return new QueryInstance(UUIDSerializer.serializer.deserialize(in, version),
                                     SerializedRequest.serializer.deserialize(in, version),
                                     CompactEndpointSerializationHelper.deserialize(in));
        }

        @Override
        public long serializedSize(QueryInstance instance, int version)
        {
            int size = 0;
            size += UUIDSerializer.serializer.serializedSize(instance.id, version);
            size += SerializedRequest.serializer.serializedSize(instance.getQuery(), version);
            size += CompactEndpointSerializationHelper.serializedSize(instance.leader);
            return size;
        }
    };

    public static final IVersionedSerializer<Instance> serializer = new IVersionedSerializer<Instance>()
    {
        private final ExternalSerializer baseSerializer = new ExternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof QueryInstance;
            commonSerializer.serialize((QueryInstance) instance, out, version);
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
            assert instance instanceof QueryInstance;
            return commonSerializer.serializedSize((QueryInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };

    public static final IVersionedSerializer<Instance> internalSerializer = new IVersionedSerializer<Instance>()
    {
        private final InternalSerializer baseSerializer = new InternalSerializer();

        @Override
        public void serialize(Instance instance, DataOutputPlus out, int version) throws IOException
        {
            assert instance instanceof QueryInstance;
            commonSerializer.serialize((QueryInstance) instance, out, version);
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
            assert instance instanceof QueryInstance;
            return commonSerializer.serializedSize((QueryInstance) instance, version)
                    + baseSerializer.serializedSize(instance, version);
        }
    };

}
