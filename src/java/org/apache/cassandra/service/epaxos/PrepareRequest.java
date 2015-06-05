package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class PrepareRequest extends AbstractEpochMessage
{
    public static final IVersionedSerializer<PrepareRequest> serializer = new Serializer();

    public final UUID iid;
    public final int ballot;

    public PrepareRequest(Token token, UUID cfId, long epoch, Instance instance)
    {
        this(token, cfId, epoch, instance.getScope(), instance.getId(), instance.getBallot());
    }

    public PrepareRequest(Token token, UUID cfId, long epoch, Scope scope, UUID iid, int ballot)
    {
        super(token, cfId, epoch, scope);
        this.iid = iid;
        this.ballot = ballot;
    }

    public MessageOut<PrepareRequest> getMessage()
    {
        return new MessageOut<>(MessagingService.Verb.EPAXOS_PREPARE, this, serializer);
    }

    @Override
    public String toString()
    {
        return "PrepareRequest{" +
               "iid=" + iid +
               ", ballot=" + ballot +
               '}';
    }

    private static class Serializer implements IVersionedSerializer<PrepareRequest>
    {
        @Override
        public void serialize(PrepareRequest request, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(request, out, version);
            UUIDSerializer.serializer.serialize(request.iid, out, version);
            out.writeInt(request.ballot);
        }

        @Override
        public PrepareRequest deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            return new PrepareRequest(epochInfo.token, epochInfo.cfId, epochInfo.epoch, epochInfo.scope,
                                      UUIDSerializer.serializer.deserialize(in, version), in.readInt());
        }

        @Override
        public long serializedSize(PrepareRequest request, int version)
        {
            return AbstractEpochMessage.serializer.serializedSize(request, version)
                    + UUIDSerializer.serializer.serializedSize(request.iid, version) + 4;
        }
    }
}
