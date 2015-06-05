package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

public class EpaxosPreacceptLeaderTest extends AbstractEpaxosIntegrationTest.SingleThread
{

    private volatile AcceptDecision lastAcceptDecision = null;

    private class Service extends Node.SingleThreaded
    {

        private Service(int number, Messenger messenger, String dc, String ksName)
        {
            super(number, messenger, dc, ksName);
        }

        @Override
        protected PreacceptCallback getPreacceptCallback(Instance instance, ParticipantInfo participantInfo, Runnable failureCallback, boolean forceAccept)
        {
            return new PreacceptCallback(this, instance, participantInfo, failureCallback, forceAccept)
            {
                @Override
                protected void processDecision(AcceptDecision decision)
                {
                    lastAcceptDecision = decision;
                }
            };
        }
    }

    @Override
    public Node createNode(int nodeNumber, Messenger messenger, String dc, String ks)
    {
        return new Service(nodeNumber, messenger, dc, ks);
    }

    @Override
    public void setUp()
    {
        super.setUp();
        lastAcceptDecision = null;
    }

    @Test
    public void replicasAgree() throws Exception
    {
        Node node = nodes.get(0);

        Instance oldInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.commit(Sets.<UUID>newHashSet());
        for (Node n: nodes)
            n.addMissingInstance(oldInstance);

        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());

        node.preaccept(instance);

        Assert.assertNotNull(lastAcceptDecision);
        AcceptDecision decision = lastAcceptDecision;

        Assert.assertEquals(Sets.newHashSet(oldInstance.getId()), instance.getDependencies());
        Assert.assertFalse(decision.acceptNeeded);
        Assert.assertEquals(instance.getDependencies(), decision.acceptDeps);
        Assert.assertEquals(Collections.EMPTY_MAP, decision.missingInstances);
    }

    @Test
    public void quorumFailure() throws Exception
    {
        setState(nodes.subList(1, nodes.size()), Node.State.DOWN);
        Node node = nodes.get(0);
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());

        node.preaccept(instance);
        Assert.assertNull(lastAcceptDecision);

        Assert.assertEquals(Sets.<UUID>newHashSet(), instance.getDependencies());
    }

    @Test
    public void disagreeingReplicas() throws Exception
    {
        Node node = nodes.get(0);

        // add an instance the leader doesn't know about
        Instance oldInstance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        oldInstance.commit(Sets.<UUID>newHashSet());
        for (Node n: nodes.subList(1, nodes.size()))
            n.addMissingInstance(oldInstance);

        Assert.assertNull(node.getInstance(oldInstance.getId()));
        Instance instance = new QueryInstance(getSerializedCQLRequest(0, 0), node.getEndpoint());
        node.preaccept(instance);

        Assert.assertNotNull(lastAcceptDecision);
        AcceptDecision decision = lastAcceptDecision;

        Assert.assertEquals(Sets.<UUID>newHashSet(), instance.getDependencies());
        Assert.assertTrue(decision.acceptNeeded);
        Assert.assertEquals(Sets.newHashSet(oldInstance.getId()), decision.acceptDeps);

        // check that we got our missing instance
        Assert.assertNotNull(node.getInstance(oldInstance.getId()));
    }

    /**
     * Once a quorum of responses is received, additional responses should be discarded
     */
    @Test
    public void lateResponseIsDiscarded() throws Exception
    {

    }
}
