package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;

/**
 * Tests replica's handling of the accept phase
 */
public class EpaxosAcceptReplicaTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private final Map<InetAddress, List<MessageOut>> replies = new HashMap<>();
    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    public void setUp()
    {
        super.setUp();
        replies.clear();
    }

    @Override
    protected Messenger createMessenger()
    {
        return new Messenger()
        {
            @Override
            public <T> void sendReply(MessageOut<T> msg, int id, InetAddress from, InetAddress to)
            {
                List<MessageOut> replyList = replies.get(from);
                if (replyList == null)
                {
                    replyList = new LinkedList<>();
                    replies.put(from, replyList);
                }
                replyList.add(msg);
            }
        };
    }

    @Test
    public void requestSuccessExisting() throws Exception
    {
        Node node = nodes.get(0);
        AcceptVerbHandler handler = (AcceptVerbHandler) node.getAcceptVerbHandler();

        // add an instance
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.preaccept(Sets.<UUID>newHashSet());
        node.addMissingInstance(instance);

        // make instance depend on a new instance
        Instance missingInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        missingInstance.accept(Sets.<UUID>newHashSet());

        instance.setDependencies(Sets.newHashSet(missingInstance.getId()));

        // check current state of instance on node
        Assert.assertNotNull(node.getInstance(instance.getId()));
        Assert.assertEquals(Instance.State.PREACCEPTED, node.getInstance(instance.getId()).getState());

        // send accept request with new deps, and missing instance
        instance.incrementBallot();
        MessageIn<AcceptRequest> message = MessageIn.create(nodes.get(1).getEndpoint(),
                                                            new AcceptRequest(instance, 0, Lists.newArrayList(missingInstance)),
                                                            Collections.EMPTY_MAP,
                                                            MessagingService.Verb.EPAXOS_ACCEPT,
                                                            0);
        handler.doVerb(message, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        AcceptResponse response = (AcceptResponse) reply.payload;
        Assert.assertTrue(response.success);

        // check instance
        Assert.assertFalse(instance == node.getInstance(instance.getId()));  // should be different instances
        Assert.assertNotNull(node.getInstance(instance.getId()));
        Assert.assertEquals(Instance.State.ACCEPTED, node.getInstance(instance.getId()).getState());
        Assert.assertEquals(instance.getDependencies(), node.getInstance(instance.getId()).getDependencies());

        // check missing instance was added
        Assert.assertNotNull(node.getInstance(missingInstance.getId()));

        // check dependency manager
        KeyState dm = node.getKeyState(instance);
        Assert.assertNotNull(dm.get(instance.getId()));
        Assert.assertNotNull(dm.get(missingInstance.getId()));

    }

    @Test
    public void requestSuccessNew() throws Exception
    {
        Node node = nodes.get(0);
        AcceptVerbHandler handler = (AcceptVerbHandler) node.getAcceptVerbHandler();

        // add an instance
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());

        // make instance depend on a new instance
        Instance missingInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        missingInstance.accept(Sets.<UUID>newHashSet());

        instance.setDependencies(Sets.newHashSet(missingInstance.getId()));

        // check current state of instance on node
        Assert.assertNull(node.getInstance(instance.getId()));

        // send accept request with new deps, and missing instance
        instance.incrementBallot();
        MessageIn<AcceptRequest> message = MessageIn.create(nodes.get(1).getEndpoint(),
                                                            new AcceptRequest(instance, 0, Lists.newArrayList(missingInstance)),
                                                            Collections.EMPTY_MAP,
                                                            MessagingService.Verb.EPAXOS_ACCEPT,
                                                            0);
        handler.doVerb(message, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        AcceptResponse response = (AcceptResponse) reply.payload;
        Assert.assertTrue(response.success);

        // check instance
        Assert.assertFalse(instance == node.getInstance(instance.getId()));  // should be different instances
        Assert.assertNotNull(node.getInstance(instance.getId()));
        Assert.assertEquals(Instance.State.ACCEPTED, node.getInstance(instance.getId()).getState());
        Assert.assertEquals(instance.getDependencies(), node.getInstance(instance.getId()).getDependencies());

        // check missing instance was added
        Assert.assertNotNull(node.getInstance(missingInstance.getId()));

        // check dependency manager
        KeyState dm = node.getKeyState(instance);
        Assert.assertNotNull(dm.get(instance.getId()));
        Assert.assertNotNull(dm.get(missingInstance.getId()));
    }

    @Test
    public void requestNoop() throws Exception
    {
        Node node = nodes.get(0);
        AcceptVerbHandler handler = (AcceptVerbHandler) node.getAcceptVerbHandler();

        // add an instance
        Instance instance1 = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance1.preaccept(Sets.<UUID>newHashSet());
        node.addMissingInstance(instance1);

        instance1.setNoop(true);
        instance1.incrementBallot();
        instance1.accept();
        MessageIn<AcceptRequest> message = MessageIn.create(nodes.get(1).getEndpoint(),
                                                            new AcceptRequest(instance1, 0, Collections.EMPTY_LIST),
                                                            Collections.EMPTY_MAP,
                                                            MessagingService.Verb.EPAXOS_ACCEPT,
                                                            0);
        handler.doVerb(message, 100);

        Assert.assertNotNull(node.getInstance(instance1.getId()));
        Assert.assertEquals(Instance.State.ACCEPTED, node.getInstance(instance1.getId()).getState());
        Assert.assertEquals(true, node.getInstance(instance1.getId()).isNoop());

        // check new instance
        Instance instance2 = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance2.accept(Sets.newHashSet(instance1.getId()));
        instance2.setNoop(true);

        message = MessageIn.create(nodes.get(1).getEndpoint(),
                                   new AcceptRequest(instance2, 0, Collections.EMPTY_LIST),
                                   Collections.EMPTY_MAP,
                                   MessagingService.Verb.EPAXOS_ACCEPT,
                                   0);

        handler.doVerb(message, 100);

        Assert.assertNotNull(node.getInstance(instance2.getId()));
        Assert.assertEquals(Instance.State.ACCEPTED, node.getInstance(instance2.getId()).getState());
        Assert.assertEquals(true, node.getInstance(instance2.getId()).isNoop());
    }

    @Test
    public void requestBallotFailure() throws Exception
    {
        Node node = nodes.get(0);
        AcceptVerbHandler handler = (AcceptVerbHandler) node.getAcceptVerbHandler();

        // add an instance
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.preaccept(Sets.<UUID>newHashSet());
        node.addMissingInstance(instance);

        // check current state of instance on node
        Assert.assertNotNull(node.getInstance(instance.getId()));
        Assert.assertEquals(Instance.State.PREACCEPTED, node.getInstance(instance.getId()).getState());

        // send accept request with new deps, and missing instance
        MessageIn<AcceptRequest> message = MessageIn.create(nodes.get(1).getEndpoint(),
                                                            new AcceptRequest(instance, 0, Collections.EMPTY_LIST),
                                                            Collections.EMPTY_MAP,
                                                            MessagingService.Verb.EPAXOS_ACCEPT,
                                                            0);
        handler.doVerb(message, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        AcceptResponse response = (AcceptResponse) reply.payload;
        Assert.assertFalse(response.success);
        Assert.assertEquals(instance.getBallot(), response.ballot);

        Assert.assertEquals(Instance.State.PREACCEPTED, node.getInstance(instance.getId()).getState());
    }

    @Test
    public void requestDepsAcknowledged() throws Exception
    {

        Node node = nodes.get(0);
        AcceptVerbHandler handler = (AcceptVerbHandler) node.getAcceptVerbHandler();

        Instance previousInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        previousInstance.preaccept(Sets.<UUID>newHashSet());
        node.addMissingInstance(previousInstance);

        // check that it's in the deps manager
        Assert.assertNotNull(node.getKeyState(previousInstance));
        Assert.assertEquals(0, node.getKeyState(previousInstance).get(previousInstance.getId()).acknowledged.size());

        // add an instance
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        instance.preaccept(Sets.<UUID>newHashSet(previousInstance.getId()));


        // send accept request with new deps, and missing instance
        instance.incrementBallot();
        MessageIn<AcceptRequest> message = MessageIn.create(nodes.get(1).getEndpoint(),
                                                            new AcceptRequest(instance, 0, Collections.EMPTY_LIST),
                                                            Collections.EMPTY_MAP,
                                                            MessagingService.Verb.EPAXOS_ACCEPT,
                                                            0);
        handler.doVerb(message, 100);

        // should now be acknowledged
        Assert.assertEquals(1, node.getKeyState(previousInstance).get(previousInstance.getId()).acknowledged.size());
    }
}
