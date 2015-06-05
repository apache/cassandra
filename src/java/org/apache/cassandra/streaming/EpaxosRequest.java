package org.apache.cassandra.streaming;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.epaxos.EpaxosService;
import org.apache.cassandra.service.epaxos.Scope;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

/**
 * Epaxos instance requests are one way
 */
public class EpaxosRequest
{
    public final UUID cfId;
    public final Range<Token> range;
    public final Scope scope;

    public EpaxosRequest(UUID cfId, Range<Token> range, Scope scope)
    {
        this.cfId = cfId;
        this.range = range;
        this.scope = scope;
    }

    public static final IVersionedSerializer<EpaxosRequest> serializer = new IVersionedSerializer<EpaxosRequest>()
    {
        @Override
        public void serialize(EpaxosRequest request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.cfId, out, version);
            Token.serializer.serialize(request.range.left, out, version);
            Token.serializer.serialize(request.range.right, out, version);
            Scope.serializer.serialize(request.scope, out, version);
        }

        @Override
        public EpaxosRequest deserialize(DataInput in, int version) throws IOException
        {
            return new EpaxosRequest(UUIDSerializer.serializer.deserialize(in, version),
                                     new Range<>(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version),
                                                 Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version)),
                                     Scope.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(EpaxosRequest request, int version)
        {
            long size = 0;
            size += UUIDSerializer.serializer.serializedSize(request.cfId, version);
            size += Token.serializer.serializedSize(request.range.left, version);
            size += Token.serializer.serializedSize(request.range.right, version);
            size += Scope.serializer.serializedSize(request.scope, version);
            return size;
        }
    };
}
