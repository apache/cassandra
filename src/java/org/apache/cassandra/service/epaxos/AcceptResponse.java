package org.apache.cassandra.service.epaxos;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInput;
import java.io.IOException;
import java.util.UUID;

public class AcceptResponse extends AbstractEpochMessage
{
    public static final IVersionedSerializer<AcceptResponse> serializer = new Serializer();
    public final boolean success;
    public final int ballot;

    public AcceptResponse(Token token, UUID cfId, long epoch, Scope scope, boolean success, int ballot)
    {
        super(token, cfId, epoch, scope);
        this.success = success;
        this.ballot = ballot;
    }

    public MessageOut<AcceptResponse> getMessage()
    {
        return new MessageOut<>(MessagingService.Verb.REQUEST_RESPONSE, this, serializer);
    }

    @Override
    public String toString()
    {
        return "AcceptResponse{" +
               "success=" + success +
               ", ballot=" + ballot +
               '}';
    }

    public static class Serializer implements IVersionedSerializer<AcceptResponse>
    {
        @Override
        public void serialize(AcceptResponse response, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(response, out, version);
            out.writeBoolean(response.success);
            out.writeInt(response.ballot);
        }

        @Override
        public AcceptResponse deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            return new AcceptResponse(epochInfo.token, epochInfo.cfId, epochInfo.epoch, epochInfo.scope, in.readBoolean(), in.readInt());
        }

        @Override
        public long serializedSize(AcceptResponse response, int version)
        {
            return AbstractEpochMessage.serializer.serializedSize(response, version) + 1 + 4;
        }
    }
}
