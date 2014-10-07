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

public class FailureRecoveryRequest
{
    public final Token token;
    public final UUID cfId;
    public final long epoch;
    public final Scope scope;

    public FailureRecoveryRequest(Token token, UUID cfId, long epoch, Scope scope)
    {
        this.token = token;
        this.cfId = cfId;
        this.epoch = epoch;
        this.scope = scope;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FailureRecoveryRequest request = (FailureRecoveryRequest) o;

        if (epoch != request.epoch) return false;
        if (!cfId.equals(request.cfId)) return false;
        if (scope != request.scope) return false;
        if (!token.equals(request.token)) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = token.hashCode();
        result = 31 * result + cfId.hashCode();
        result = 31 * result + (int) (epoch ^ (epoch >>> 32));
        result = 31 * result + scope.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "FailureRecoveryRequest{" +
               "token=" + token +
               ", cfId=" + cfId +
               ", epoch=" + epoch +
               ", scope=" + scope +
               '}';
    }

    public static final IVersionedSerializer<FailureRecoveryRequest> serializer = new IVersionedSerializer<FailureRecoveryRequest>()
    {
        @Override
        public void serialize(FailureRecoveryRequest request, DataOutputPlus out, int version) throws IOException
        {
            Token.serializer.serialize(request.token, out, version);
            UUIDSerializer.serializer.serialize(request.cfId, out, version);
            out.writeLong(request.epoch);
            Scope.serializer.serialize(request.scope, out, version);
        }

        @Override
        public FailureRecoveryRequest deserialize(DataInput in, int version) throws IOException
        {
            return new FailureRecoveryRequest(Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version),
                                              UUIDSerializer.serializer.deserialize(in, version),
                                              in.readLong(),
                                              Scope.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(FailureRecoveryRequest request, int version)
        {
            return Token.serializer.serializedSize(request.token, version)
                    + UUIDSerializer.serializer.serializedSize(request.cfId, version)
                    + 8
                    + Scope.serializer.serializedSize(request.scope, version);
        }
    };
}
