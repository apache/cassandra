package org.apache.cassandra.service.epaxos.integration;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.epaxos.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EpaxosEpochIntegrationTest extends AbstractEpaxosIntegrationTest.SingleThread
{
    private static final Token TOKEN = DatabaseDescriptor.getPartitioner().getToken(ByteBufferUtil.bytes(1234));

    @Override
    protected Messenger createMessenger()
    {
        return new Messenger(Node.queuedExecutor);
    }

    @Test
    public void successCase() throws Exception
    {
        int roundSize = 10;

        // sanity check
        for (Node node: nodes)
        {
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
            node.setEpochIncrementThreshold(roundSize, DEFAULT_SCOPE);
        }

        Set<UUID> round1Ids = Sets.newHashSet();
        for (int i=0; i<roundSize; i++)
        {
            Node leader = nodes.get(i % nodes.size());
            leader.query(getSerializedCQLRequest(i, 0));
            Instance instance = leader.getLastCreatedInstance();
            round1Ids.add(instance.getId());
        }

        Instance lastEpochInstance;
        {
            Node leader = nodes.get(0);
            TokenStateMaintenanceTask maintenanceTask = leader.newTokenStateMaintenanceTask();
            maintenanceTask.run();

            for (Node node: nodes)
            {
                Assert.assertEquals(1, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
            }

            Assert.assertTrue(leader.getLastCreatedInstance() instanceof EpochInstance);
            EpochInstance instance = (EpochInstance) leader.getLastCreatedInstance();

            Assert.assertEquals(1, instance.getEpoch());
            Assert.assertFalse(instance.isVetoed());

            // should have taken all of the first round of instances
            // as deps since they all targeted a different key
            Assert.assertEquals(round1Ids, instance.getDependencies());

            lastEpochInstance = instance;
        }

        Set<UUID> round2Ids = Sets.newHashSet();
        for (int i=0; i<roundSize; i++)
        {
            Node leader = nodes.get(i % nodes.size());
            leader.query(getSerializedCQLRequest(i + roundSize, 0));
            Instance instance = leader.getLastCreatedInstance();
            round2Ids.add(instance.getId());
        }

        {
            Node leader = nodes.get(0);
            TokenStateMaintenanceTask maintenanceTask = leader.newTokenStateMaintenanceTask();
            maintenanceTask.run();

            for (Node node: nodes)
            {
                Assert.assertEquals(2, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
            }

            Assert.assertTrue(leader.getLastCreatedInstance() instanceof EpochInstance);
            EpochInstance instance = (EpochInstance) leader.getLastCreatedInstance();

            Assert.assertEquals(2, instance.getEpoch());
            Assert.assertFalse(instance.isVetoed());

            // should have taken all of the first round of instances
            // as deps since they all targeted a different key
            Set<UUID> expectedDeps = Sets.newHashSet(round2Ids);
            expectedDeps.add(lastEpochInstance.getId());

            Set<UUID> oldDeps = Sets.intersection(round1Ids, instance.getDependencies());
            Assert.assertEquals(0, oldDeps.size());
            Assert.assertEquals(expectedDeps, instance.getDependencies());
        }
    }

    /**
     * If a query instance for a given key in epoch 0 is strongly connected with an epoch
     * instance, incrementing the epoch to 1, and that key has no activity in the next epoch (1)
     * the epoch instance that increments to epoch 2 will be dependent on that key
     *
     * Making the epoch increment instance part of the epoch it increments out of would fix this
     * issue, so we could keep 2 epochs around, not 3
     */
    @Test
    public void stronglyConnectedEpochInstance() throws Exception
    {
        for (Node node: nodes)
        {
            node.setEpochIncrementThreshold(0, DEFAULT_SCOPE);
            Assert.assertEquals(0, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
        }

        // submit a query and epoch increment "in parallel" to form a strongly connected component
        Node.queuedExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                Node.queuedExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            nodes.get(0).query(getSerializedCQLRequest(0, 0));
                        }
                        catch (ReadTimeoutException | WriteTimeoutException | UnavailableException | InvalidRequestException e)
                        {
                            // the query will think it's timed out
                        }
                    }
                });

                Node.queuedExecutor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        nodes.get(1).newTokenStateMaintenanceTask().run();
                    }
                });
            }
        });

        QueryInstance queryInstance = (QueryInstance) nodes.get(0).getLastCreatedInstance();
        EpochInstance epochInstance = (EpochInstance) nodes.get(1).getLastCreatedInstance();

        Assert.assertNotNull(queryInstance);
        Assert.assertNotNull(epochInstance);

        Assert.assertEquals(Sets.newHashSet(epochInstance.getId()), queryInstance.getDependencies());
        Assert.assertEquals(Sets.newHashSet(queryInstance.getId()), epochInstance.getDependencies());

        for (Node node: nodes)
        {
            Assert.assertEquals(1, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
            KeyState keyState = node.getKeyState(queryInstance);
            Map<Long, Set<UUID>> epochs = keyState.getEpochExecutions();
            Assert.assertTrue(epochs.get(0l).contains(queryInstance.getId()));
            Assert.assertTrue(epochs.get(1l).contains(epochInstance.getId()));
        }

        Node leader = nodes.get(0);
        TokenStateMaintenanceTask maintenanceTask = leader.newTokenStateMaintenanceTask();
        maintenanceTask.run();

        for (Node node: nodes)
        {
            Assert.assertEquals(2, node.getCurrentEpoch(TOKEN, cfm.cfId, DEFAULT_SCOPE));
        }

        Assert.assertTrue(leader.getLastCreatedInstance() instanceof EpochInstance);
        EpochInstance instance = (EpochInstance) leader.getLastCreatedInstance();

        Assert.assertEquals(2, instance.getEpoch());
        Assert.assertFalse(instance.isVetoed());

        // should have taken all of the first round of instances
        // as deps since they all targeted a different key
        Assert.assertEquals(Sets.newHashSet(epochInstance.getId()), instance.getDependencies());
    }

    @Test
    public void nonreplicatedTokenStatesAreSkipped()
    {

    }
}
