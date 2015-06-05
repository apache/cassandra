package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.*;

public class EpaxosPreacceptReplicaTest extends AbstractEpaxosIntegrationTest.SingleThread
{

    private final Map<InetAddress, List<MessageOut>> replies = new HashMap<>();

    @Override
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
    public void matchingDependencies() throws Exception
    {
        // create an instance that only node0 knows about
        Node node = nodes.get(0);
        Instance firstInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        firstInstance.commit(Sets.<UUID>newHashSet());
        node.addMissingInstance(firstInstance);


        // send node 0 a preaccept message that doesn't include the missing instance
        Instance newInstance = new QueryInstance(getSerializedCQLRequest(0, 0), nodes.get(1).getEndpoint());
        newInstance.preaccept(Sets.newHashSet(firstInstance.getId()));

        MessageIn<MessageEnvelope<Instance>> request = MessageIn.create(nodes.get(1).getEndpoint(),
                                                                        wrapInstance(newInstance),
                                                                        Collections.EMPTY_MAP,
                                                                        MessagingService.Verb.EPAXOS_PREACCEPT,
                                                                        100);

        PreacceptVerbHandler preacceptHandler = (PreacceptVerbHandler) node.getPreacceptVerbHandler();

        preacceptHandler.doVerb(request, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        PreacceptResponse response = (PreacceptResponse) reply.payload;

        Assert.assertTrue(response.success);
        Assert.assertEquals(0, response.missingInstances.size());
        Assert.assertEquals(Sets.newHashSet(firstInstance.getId()), response.dependencies);

        // check node state
        Instance remoteCopy = node.getInstance(newInstance.getId());
        Assert.assertNotNull(remoteCopy);
        Assert.assertEquals(Instance.State.PREACCEPTED, remoteCopy.getState());
        Assert.assertEquals(Sets.newHashSet(firstInstance.getId()), remoteCopy.getDependencies());
        Assert.assertTrue(remoteCopy.getLeaderAttrsMatch());

        // check added to dependency manager
        KeyState dm = node.getKeyState(newInstance);
        KeyState.Entry entry = dm.get(newInstance.getId());
        Assert.assertNotNull(entry);
    }

    @Test
    public void missingReplicaDependency() throws Exception
    {
        // create an instance that all nodes know
        Node node = nodes.get(0);
        Instance firstInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        firstInstance.commit(Sets.<UUID>newHashSet());
        node.addMissingInstance(firstInstance);


        // send node 0 a preaccept message that doesn't include the missing instance
        Instance newInstance = new QueryInstance(getSerializedCQLRequest(0, 0), nodes.get(1).getEndpoint());
        newInstance.preaccept(Sets.newHashSet(firstInstance.getId(), UUIDGen.getTimeUUID()));

        MessageIn<MessageEnvelope<Instance>> request = MessageIn.create(nodes.get(1).getEndpoint(),
                                                                        wrapInstance(newInstance),
                                                                        Collections.EMPTY_MAP,
                                                                        MessagingService.Verb.EPAXOS_PREACCEPT,
                                                                        100);

        PreacceptVerbHandler preacceptHandler = (PreacceptVerbHandler) node.getPreacceptVerbHandler();

        preacceptHandler.doVerb(request, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        PreacceptResponse response = (PreacceptResponse) reply.payload;

        Assert.assertFalse(response.success);
        Assert.assertEquals(0, response.missingInstances.size());
        Assert.assertEquals(Sets.newHashSet(firstInstance.getId()), response.dependencies);

        // check node state
        Instance remoteCopy = node.getInstance(newInstance.getId());
        Assert.assertNotNull(remoteCopy);
        Assert.assertEquals(Instance.State.PREACCEPTED, remoteCopy.getState());
        Assert.assertEquals(Sets.newHashSet(firstInstance.getId()), remoteCopy.getDependencies());
        Assert.assertFalse(remoteCopy.getLeaderAttrsMatch());
    }

    @Test
    public void missingLeaderDependency() throws Exception
    {
        // create an instance that only node0 knows about
        Node node = nodes.get(0);
        Instance missingInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        missingInstance.commit(Sets.<UUID>newHashSet());
        node.addMissingInstance(missingInstance);


        // send node 0 a preaccept message that doesn't include the missing instance
        Instance newInstance = new QueryInstance(getSerializedCQLRequest(0, 0), nodes.get(1).getEndpoint());
        newInstance.preaccept(Sets.<UUID>newHashSet());

        MessageIn<MessageEnvelope<Instance>> request = MessageIn.create(nodes.get(1).getEndpoint(),
                                                                        wrapInstance(newInstance),
                                                                        Collections.EMPTY_MAP,
                                                                        MessagingService.Verb.EPAXOS_PREACCEPT,
                                                                        100);

        PreacceptVerbHandler preacceptHandler = (PreacceptVerbHandler) node.getPreacceptVerbHandler();

        preacceptHandler.doVerb(request, 100);

        // check the response
        Assert.assertTrue(replies.containsKey(node.getEndpoint()));
        Assert.assertEquals(1, replies.get(node.getEndpoint()).size());

        MessageOut reply = replies.get(node.getEndpoint()).get(0);
        PreacceptResponse response = (PreacceptResponse) reply.payload;

        Assert.assertFalse(response.success);
        Assert.assertEquals(1, response.missingInstances.size());
        Assert.assertEquals(missingInstance.getId(), response.missingInstances.get(0).getId());
        Assert.assertEquals(Sets.newHashSet(missingInstance.getId()), response.dependencies);

        // check node state
        Instance remoteCopy = node.getInstance(newInstance.getId());
        Assert.assertNotNull(remoteCopy);
        Assert.assertEquals(Instance.State.PREACCEPTED, remoteCopy.getState());
        Assert.assertEquals(Sets.newHashSet(missingInstance.getId()), remoteCopy.getDependencies());
        Assert.assertFalse(remoteCopy.getLeaderAttrsMatch());
    }
}
