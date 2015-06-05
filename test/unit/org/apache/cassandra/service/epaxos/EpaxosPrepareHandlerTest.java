package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.UUID;

public class EpaxosPrepareHandlerTest extends AbstractEpaxosTest
{
    static final InetAddress LOCAL;
    static final InetAddress LEADER;
    static final String LEADER_DC = "DC1";

    static
    {
        try
        {
            LOCAL = InetAddress.getByAddress(new byte[]{0, 0, 0, 1});
            LEADER = InetAddress.getByAddress(new byte[]{0, 0, 0, 2});
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    MessageIn<PrepareRequest> createMessage(Instance instance)
    {
        return createMessage(instance.getId(), instance.getBallot());
    }

    MessageIn<PrepareRequest> createMessage(UUID id, int ballot)
    {
        return MessageIn.create(LEADER,
                                new PrepareRequest(TOKEN0, CFID, 0, DEFAULT_SCOPE, id, ballot),
                                Collections.<String, byte[]>emptyMap(),
                                MessagingService.Verb.EPAXOS_ACCEPT,
                                0);
    }
    @Test
    public void successCase() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(1, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = service.replies.get(0);
        Assert.assertEquals(instance.getBallot(), message.payload.contents.getBallot());
    }

    @Test
    public void unknownInstance() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = service.replies.get(0);
        Assert.assertNull(message.payload.contents);
    }

    @Test
    public void ballotFailure() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());
        service.instanceMap.get(instance.getId()).updateBallot(20);

        handler.doVerb(createMessage(instance), 0);

        // response is the same, except the instance isn't saved
        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = service.replies.get(0);
        Assert.assertEquals(20, message.payload.contents.getBallot());
    }

    @Test
    public void placeholderInstancesArentReturnedWithNonZeroBallot() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());
        service.instanceMap.get(instance.getId()).setPlaceholder(true);

        instance.incrementBallot();
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = service.replies.get(0);
        Assert.assertNull(message.payload.contents);
    }

    @Test
    public void placeholderInstancesAreReturnedWithZeroBallot() throws Exception
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), LEADER);
        instance.preaccept(Sets.newHashSet(UUIDGen.getTimeUUID()));

        service.instanceMap.put(instance.getId(), instance.copy());
        service.instanceMap.get(instance.getId()).setPlaceholder(true);

        instance.ballot = 0;
        handler.doVerb(createMessage(instance), 0);

        Assert.assertEquals(0, service.savedInstances.size());
        Assert.assertEquals(1, service.replies.size());

        MessageOut<MessageEnvelope<Instance>> message = service.replies.get(0);
        Assert.assertNotNull(message.payload.contents);
    }

    @Test
    public void passiveRecord()
    {
        MockVerbHandlerService service = new MockVerbHandlerService();
        PrepareVerbHandler handler = new PrepareVerbHandler(service);
        Assert.assertFalse(handler.canPassiveRecord());
    }
}
