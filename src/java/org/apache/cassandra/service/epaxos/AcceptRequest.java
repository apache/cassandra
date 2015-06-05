package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class AcceptRequest extends AbstractEpochMessage
{
    public static final IVersionedSerializer<AcceptRequest> serializer = new Serializer();

    public final Instance instance;
    public final List<Instance> missingInstances;

    public AcceptRequest(Instance instance, long epoch, List<Instance> missingInstances)
    {
        super(instance.getToken(), instance.getCfId(), epoch, instance.getScope());
        this.instance = instance;
        this.missingInstances = missingInstances != null ? missingInstances : Collections.<Instance>emptyList();
    }

    public AcceptRequest(AbstractEpochMessage epochInfo, Instance instance, List<Instance> missingInstances)
    {
        super(epochInfo);
        this.instance = instance;
        this.missingInstances = missingInstances != null ? missingInstances : Collections.<Instance>emptyList();
    }

    public MessageOut<AcceptRequest> getMessage()
    {
        return new MessageOut<>(MessagingService.Verb.EPAXOS_ACCEPT, this, serializer);
    }

    @Override
    public String toString()
    {
        return "AcceptRequest{" +
               "instance=" + instance.getId() +
               ", missingInstances=" + missingInstances.size() +
               '}';
    }

    private static class Serializer implements IVersionedSerializer<AcceptRequest>
    {
        @Override
        public void serialize(AcceptRequest request, DataOutputPlus out, int version) throws IOException
        {
            AbstractEpochMessage.serializer.serialize(request, out, version);
            Instance.serializer.serialize(request.instance, out, version);
            out.writeInt(request.missingInstances.size());
            for (Instance missing: request.missingInstances)
                Instance.serializer.serialize(missing, out, version);
        }

        @Override
        public AcceptRequest deserialize(DataInput in, int version) throws IOException
        {
            AbstractEpochMessage epochInfo = AbstractEpochMessage.serializer.deserialize(in, version);
            Instance instance = Instance.serializer.deserialize(in, version);
            int numMissing = in.readInt();
            List<Instance> missingInstances = Lists.newArrayListWithCapacity(numMissing);
            for (int i=0; i<numMissing; i++)
                missingInstances.add(Instance.serializer.deserialize(in, version));
            return new AcceptRequest(epochInfo, instance, missingInstances);
        }

        @Override
        public long serializedSize(AcceptRequest request, int version)
        {
            long size = AbstractEpochMessage.serializer.serializedSize(request, version);
            size += Instance.serializer.serializedSize(request.instance, version);
            size += 4;
            for (Instance missing: request.missingInstances)
                size += Instance.serializer.serializedSize(missing, version);

            return size;
        }
    }
}
