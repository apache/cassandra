package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.epaxos.Instance;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

public class EpaxosIntegrationRF3Test extends AbstractEpaxosIntegrationTest.SingleThread
{

    /**
     * All nodes are replying to messages
     */
    @Test
    public void successCase() throws Exception
    {
        Node leader1 = nodes.get(0);
        leader1.query(getSerializedCQLRequest(1, 2));
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceDeps(instance1.getId(), nodes, expectedDeps);
        assertInstanceState(instance1.getId(), nodes, Instance.State.EXECUTED);
        Assert.assertFalse(leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedCQLRequest(1, 2));
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceDeps(instance2.getId(), nodes, expectedDeps);
        assertInstanceState(instance2.getId(), nodes, Instance.State.EXECUTED);
        Assert.assertFalse(leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(nodes, expectedOrder);
    }

    /**
     * only a quorum of nodes respond
     */
    @Test
    public void quorumSuccessCase() throws Exception
    {
        List<Node> up = nodes.subList(0, quorumSize());
        List<Node> down = nodes.subList(quorumSize(), nodes.size());
        setState(up, Node.State.UP);
        setState(down, Node.State.DOWN);

        boolean acceptExpected = fastPathQuorumSize() > quorumSize();

        Node leader1 = nodes.get(0);
        leader1.query(getSerializedThriftRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceUnknown(instance1.getId(), down);
        assertInstanceDeps(instance1.getId(), up, expectedDeps);
        assertInstanceState(instance1.getId(), up, Instance.State.EXECUTED);
        Assert.assertEquals(acceptExpected, leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedThriftRequest());
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceUnknown(instance2.getId(), down);
        assertInstanceDeps(instance2.getId(), up, expectedDeps);
        assertInstanceState(instance2.getId(), up, Instance.State.EXECUTED);
        Assert.assertEquals(acceptExpected, leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(up, expectedOrder);
    }

    /**
     * only a fast quorum of nodes respond
     */
    @Test
    public void fastQuorumSuccessCase() throws Exception
    {
        List<Node> up = nodes.subList(0, fastPathQuorumSize());
        List<Node> down = nodes.subList(fastPathQuorumSize(), nodes.size());
        setState(up, Node.State.UP);
        setState(down, Node.State.DOWN);

        Node leader1 = nodes.get(0);
        leader1.query(getSerializedThriftRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        Set<UUID> expectedDeps = new HashSet<>();

        assertInstanceUnknown(instance1.getId(), down);
        assertInstanceDeps(instance1.getId(), up, expectedDeps);
        assertInstanceState(instance1.getId(), up, Instance.State.EXECUTED);
        Assert.assertFalse(leader1.accepted.contains(instance1.getId()));

        Node leader2 = nodes.get(1);
        leader2.query(getSerializedThriftRequest());
        Instance instance2 = leader2.getLastCreatedInstance();
        expectedDeps.add(instance1.getId());

        assertInstanceUnknown(instance2.getId(), down);
        assertInstanceDeps(instance2.getId(), up, expectedDeps);
        assertInstanceState(instance2.getId(), up, Instance.State.EXECUTED);
        Assert.assertFalse(leader2.accepted.contains(instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance1.getId(), instance2.getId());
        assertExecutionOrder(up, expectedOrder);
    }

    /**
     * Tests an accept is sent if the leader get's different deps back
     */
    @Test
    public void accept() throws Exception
    {
        Node leader = nodes.get(0);
        setState(Collections.singleton(leader), Node.State.DOWN);

        Node missedLeader = nodes.get(nodes.size() - 1);
        missedLeader.query(getSerializedCQLRequest(0, 0));
        Instance missedInstance = missedLeader.getLastCreatedInstance();

        assertInstanceState(missedInstance.getId(), nodes.subList(1, nodes.size()), Instance.State.EXECUTED);
        assertInstanceDeps(missedInstance.getId(), nodes.subList(1, nodes.size()), Sets.<UUID>newHashSet());
        Assert.assertEquals(0, missedLeader.accepted.size());

        setState(Collections.singleton(leader), Node.State.UP);
        assertInstanceUnknown(missedInstance.getId(), Collections.singleton(leader));

        leader.query(getSerializedCQLRequest(0, 0));
        Instance instance = leader.getLastCreatedInstance();
        assertInstanceState(instance.getId(), nodes, Instance.State.EXECUTED);
        assertInstanceDeps(instance.getId(), nodes, Sets.newHashSet(missedInstance.getId()));
        Assert.assertTrue(leader.accepted.contains(instance.getId()));
    }

    /**
     * Tests failure recovery in the following scenario
     *
     * 1: Assuming a cluster with replicas numbered 0-N. Replica N attempts to run an instance
     * with only the fast quorum minority responding (just itself for RF3, itself and another for
     * RF5, and so on). The query is aborted since a basic quorum didn't respond.
     *
     * 2: Replica 0 runs an instance of epaxos. It receives identical attributes from a fast path quorum,
     * which are also the nodes that weren't reached in query 1. The minority nodes receive and update
     * the preaccept response, but replica 0 doesn't receive them. Upon receiving a fast path quorum of
     * identical responses, it commits and executes the query, responds with success to the client, and
     * asyncronously notifies all other replicas of the commit. However, None of the other nodes receive
     * the commit.
     *
     * 3: Replica N attempts to run an instance. It can still contact the other minority node from step 1
     * (if any), and can also contact (only) one of the non-leader replicas from step 2, which never received
     * a commit message for step 2. When replica N goes to commit and execute this new instance, it should run
     * a prepare phase for the instances from step 1 & 2. It MUST commit the instance from step 2 with no
     * dependencies. The instance from step 1 may be committed with a noop, but should come after the instance
     * from step 2 in the execution order. The instance from step 3 should be executed last.
     */
    @Test
    public void inferredFastPathFailedLeaderRecovery() throws Exception
    {
        // first, preaccept an instance on a fast path minority of the nodes
        setState(nodes.subList(0, fastPathQuorumSize()), Node.State.DOWN);
        Node leader1 = nodes.get(nodes.size() - 1);
        try
        {
            leader1.query(getSerializedThriftRequest());
            Assert.fail("expecting WriteTimeoutException");
        }
        catch (WriteTimeoutException e)
        {
            // as expected
        }
        Instance instance1 = leader1.getLastCreatedInstance();

        assertInstanceUnknown(instance1.getId(), nodes.subList(0, fastPathQuorumSize()));
        Set<UUID> expectedDeps = Sets.newHashSet();
        assertInstanceDeps(instance1.getId(), nodes.subList(fastPathQuorumSize(), nodes.size()), expectedDeps);
        assertInstanceState(instance1.getId(), nodes.subList(fastPathQuorumSize(), nodes.size()), Instance.State.PREACCEPTED);

        // second, a fast path of replicas responds with identical attributes, but the fast path minority
        // responds with non matching attributes. The leader doesn't receive the dissenting responses,
        // resulting in a fast path commit, but the non-leaders don't receive the commit message
        Node leader2 = nodes.get(0);
        setState(nodes, Node.State.UP);
        setState(nodes.subList(fastPathQuorumSize(), nodes.size()), Node.State.NORESPONSE);

        // kill all replicas except the leader once the preaccept phase completes
        leader2.preCommitHook = new Runnable()
        {
            @Override
            public void run()
            {
                setState(nodes.subList(1, nodes.size()), Node.State.DOWN);
            }
        };
        leader2.query(getSerializedThriftRequest());
        Instance instance2 = leader2.getLastCreatedInstance();

        // leader instance should be executed
        assertInstanceState(instance2.getId(), nodes.subList(0, 1), Instance.State.EXECUTED);
        // all other nodes should have preaccepted
        assertInstanceState(instance2.getId(), nodes.subList(1, nodes.size()), Instance.State.PREACCEPTED);
        // fast path remainder should have no deps
        assertInstanceDeps(instance2.getId(), nodes.subList(0, fastPathQuorumSize()), Sets.<UUID>newHashSet());
        assertInstanceLeaderDepsMatch(instance2.getId(), nodes.subList(1, fastPathQuorumSize()), true);
        // fast path minority should depend on the failed instance
        assertInstanceDeps(instance2.getId(), nodes.subList(fastPathQuorumSize(), nodes.size()), Sets.newHashSet(instance1.getId()));
        assertInstanceLeaderDepsMatch(instance2.getId(), nodes.subList(fastPathQuorumSize(), nodes.size()), false);

        // third, switch the partition, so all nodes that contributed to the second commit are down, with
        // the exception of one which didn't commit, and run an instance. When this instance is executed,
        // the uncommitted instance from the previous run will be prepared, and the prepare stage should be
        // able to infer that the second instance was committed on the fast path, and shouldn't restart the
        // preaccept phase
        setState(nodes, Node.State.DOWN);
        setState(nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Node.State.UP);
        leader1.query(getSerializedThriftRequest());
        Instance instance3 = leader1.getLastCreatedInstance();

        // instance2 was committed on node 0 with no deps, and the other nodes must have as well
        assertInstanceState(instance2.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Instance.State.EXECUTED);
        assertInstanceDeps(instance2.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Sets.<UUID>newHashSet());

        // instance1 should have been committed with committed instance as a dependency
        assertInstanceState(instance1.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Instance.State.EXECUTED);
        assertInstanceDeps(instance1.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Sets.newHashSet(instance2.getId(), instance3.getId()));

        // instance3 shoudl have both instances as a dependnecy
        assertInstanceState(instance3.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Instance.State.EXECUTED);
        assertInstanceDeps(instance3.getId(), nodes.subList(fastPathQuorumSize() - 1, nodes.size()), Sets.newHashSet(instance1.getId(), instance2.getId()));

        List<UUID> expectedOrder = Lists.newArrayList(instance2.getId(), instance1.getId(), instance3.getId());
        assertExecutionOrder(nodes.subList(fastPathQuorumSize() - 1, nodes.size()), expectedOrder);
    }

    /**
     * Tests the leader getting positive responses from all nodes, but going down before sending
     * commit requests. The other nodes should be able to work out that the leader would have
     * committed;
     */
    @Test
    public void inferredFastPathFailedLeaderRecovery2() throws Exception
    {
        // first preaccept an instance, but kill the leader before it can notify
        // the other nodes of success
        final Node leader1 = nodes.get(0);
        leader1.preCommitHook = new Runnable()
        {
            @Override
            public void run()
            {
                leader1.setState(Node.State.DOWN);
            }
        };
        leader1.query(getSerializedThriftRequest());
        Instance instance1 = leader1.getLastCreatedInstance();
        List<Node> upNodes = nodes.subList(1, nodes.size());

        assertInstanceState(instance1.getId(), Collections.singleton(leader1), Instance.State.EXECUTED);
        assertInstanceState(instance1.getId(), upNodes, Instance.State.PREACCEPTED);
        assertInstanceDeps(instance1.getId(), nodes, Collections.<UUID>emptySet());

        // next, run an instance on one of the other nodes, the original instance should
        // be prepared and have the same dependencies as the leader
        Node leader2 = nodes.get(nodes.size() - 1);
        leader2.query(getSerializedThriftRequest());
        assertInstanceState(instance1.getId(), upNodes, Instance.State.EXECUTED);
        assertInstanceDeps(instance1.getId(), upNodes, Collections.<UUID>emptySet());
    }
}
