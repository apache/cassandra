package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.service.epaxos.exceptions.InvalidInstanceStateChange;
import org.apache.cassandra.service.epaxos.integration.AbstractEpaxosIntegrationTest;
import org.apache.cassandra.service.epaxos.integration.Messenger;
import org.apache.cassandra.service.epaxos.integration.Node;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class EpaxosInstanceStreamingTest extends AbstractEpaxosIntegrationTest
{

    private static final IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
    private static final Set<UUID> EMPTY = Collections.emptySet();

    private static class Service extends Node.SingleThreaded
    {

        private Service(int number, Messenger messenger, String dc, String ksName)
        {
            super(number, messenger, dc, ksName);
        }

        @Override
        protected void scheduleTokenStateMaintenanceTask()
        {
            // no-op
        }

        @Override
        protected TokenStateManager createTokenStateManager(Scope scope)
        {
            return new EpaxosTokenIntegrationTest.IntegrationTokenStateManager(getKeyspace(), getTokenStateTable(), scope);
        }
    }

    @Override
    public Node createNode(int number, Messenger messenger, String dc, String ks)
    {
        return new Service(number, messenger, dc, ks);
    }

    private QueryInstance newInstance(EpaxosService service, int key, ConsistencyLevel cl)
    {
        QueryInstance instance = service.createQueryInstance(getSerializedCQLRequest(key, key, cl));
        instance.setDependencies(EMPTY);
        return instance;
    }


    private UUID executeInstance(int k, long epoch, EpaxosService service, Scope scope)
    {
        Instance instance = newInstance(service, k, scope.cl);
        instance.setDependencies(service.getCurrentDependencies(instance).left);
        instance.setExecuted(epoch);
        service.saveInstance(instance);

        ByteBuffer key = ByteBufferUtil.bytes(k);

        KeyState ks = service.getKeyStateManager(scope).loadKeyState(key, cfm.cfId);
        ks.markExecuted(instance.getId(), EMPTY, epoch, null, 0);
        ks.markAcknowledged(instance.getDependencies(), instance.getId());
        service.getKeyStateManager(scope).saveKeyState(key, cfm.cfId, ks);
        return instance.getId();
    }

    private UUID activeInstance(int k, EpaxosService service) throws InvalidInstanceStateChange
    {
        Instance instance = newInstance(service, k, ConsistencyLevel.SERIAL);
        instance.commit(service.getCurrentDependencies(instance).left);
        service.saveInstance(instance);

        ByteBuffer key = ByteBufferUtil.bytes(k);

        KeyState ks = service.getKeyStateManager(DEFAULT_SCOPE).loadKeyState(key, cfm.cfId);
        ks.recordInstance(instance.getId());
        ks.markAcknowledged(instance.getDependencies(), instance.getId());
        service.getKeyStateManager(DEFAULT_SCOPE).saveKeyState(key, cfm.cfId, ks);
        return instance.getId();
    }

    private Set<UUID> getAllInstanceIds(EpaxosService service)
    {
        UntypedResultSet results = QueryProcessor.executeInternal(String.format("SELECT id FROM %s.%s",
                                                                                service.getKeyspace(),
                                                                                service.getInstanceTable()));
        Set<UUID> ids = Sets.newHashSet();
        for (UntypedResultSet.Row row: results)
        {
            ids.add(row.getUUID("id"));
        }

        return ids;
    }

    private static void assertKeyAndTokenStateEpoch(EpaxosService service, ByteBuffer key, UUID cfId, long epoch, Scope scope)
    {
        KeyState ks = service.getKeyStateManager(scope).loadKeyState(key, cfId);
        TokenState ts = service.getTokenStateManager(scope).get(key, cfId);
        Assert.assertEquals(String.format("Key: %s", Hex.bytesToHex(key.array())), epoch, ks.getEpoch());
        Assert.assertEquals(String.format("Key: %s", Hex.bytesToHex(key.array())), epoch, ts.getEpoch());
    }

    /**
     * Test token states being streamed
     * from one node to another
     */
    @Test
    public void successCase() throws Exception
    {
        UUID cfId = cfm.cfId;
        Node fromNode = nodes.get(0);
        Node toNode = nodes.get(1);

        Token token100 = partitioner.getToken(ByteBufferUtil.bytes(100));
        Token token200 = partitioner.getToken(ByteBufferUtil.bytes(200));
        Token token300 = partitioner.getToken(ByteBufferUtil.bytes(300));

        for (Scope scope: Scope.BOTH)
        {
            TokenStateManager fTsm = fromNode.getTokenStateManager(scope);
            TokenStateManager.ManagedCf fCf = fTsm.getOrInitManagedCf(cfId);
            fCf.putIfAbsent(new TokenState(range(token100, token200), cfId, 10, 0));
            fCf.putIfAbsent(new TokenState(range(token200, token300), cfId, 20, 0));
        }

        Set<UUID> includedIds = Sets.newHashSet();
        Set<UUID> excludedIds = Sets.newHashSet();

        for (int k=50; k<=350; k+=50)
        {
            long epoch = k > 200 && k <= 300 ? 20 : 10;

            boolean included = k > 100 && k <= 300;
            ByteBuffer key = ByteBufferUtil.bytes(k);

            assertKeyAndTokenStateEpoch(fromNode, key, cfId, epoch, Scope.GLOBAL);

            // execute and instance in epoch, epoch -1, and epoch -2
            // and add an 'active' instance
            excludedIds.add(executeInstance(k, epoch - 2, fromNode, Scope.GLOBAL));

            Set<UUID> maybeTransmitted = Sets.newHashSet();
            maybeTransmitted.add(executeInstance(k, epoch - 1, fromNode, Scope.GLOBAL));
            maybeTransmitted.add(executeInstance(k, epoch, fromNode, Scope.GLOBAL));
            maybeTransmitted.add(activeInstance(k, fromNode));
            if (included)
            {
                includedIds.addAll(maybeTransmitted);
            }
            else
            {
                excludedIds.addAll(maybeTransmitted);
            }

            // should be excluded from the stream
            excludedIds.add(executeInstance(k, epoch, fromNode, Scope.LOCAL));
        }

        final Range<Token> range = new Range<>(token100, token300);

        InstanceStreamWriter writer = new InstanceStreamWriter(fromNode, cfId, range, Scope.GLOBAL, toNode.getEndpoint());
        InstanceStreamReader reader = new InstanceStreamReader(toNode, cfId, range, Scope.GLOBAL, fromNode.getEndpoint());

        DataOutputBuffer outputBuffer = new DataOutputBuffer();
        writer.write(outputBuffer);

        ReadableByteChannel inputChannel = Channels.newChannel(new ByteArrayInputStream(outputBuffer.getData()));
        reader.read(inputChannel, null);

        TokenStateManager tTsm = fromNode.getTokenStateManager(Scope.GLOBAL);
        TokenStateManager.ManagedCf tCf = tTsm.getOrInitManagedCf(cfId);
        Assert.assertEquals(2, tCf.allTokens().size());

        TokenState ts200 = tTsm.getExact(token200, cfId);
        Assert.assertNotNull(ts200);
        Assert.assertEquals(10, ts200.getEpoch());

        TokenState ts300 = tTsm.getExact(token300, cfId);
        Assert.assertNotNull(ts300);
        Assert.assertEquals(20, ts300.getEpoch());

        Set<UUID> actualIds = getAllInstanceIds(toNode);
        Assert.assertEquals(includedIds, actualIds);

        // check that all keys are accounted for in the to-node's key states
        Set<UUID> expectedIds = Sets.newHashSet(actualIds);
        for (int k=150; k<=300; k+=50)
        {
            KeyState ks = toNode.getKeyStateManager(Scope.GLOBAL).loadKeyState(ByteBufferUtil.bytes(k), cfId);
            Assert.assertNotNull(ks);
            Set<UUID> activeIds = ks.getActiveInstanceIds();
            Assert.assertEquals(1, activeIds.size());
            Assert.assertTrue(expectedIds.containsAll(activeIds));
            expectedIds.removeAll(activeIds);

            Map<Long, Set<UUID>> epochExecutions = ks.getEpochExecutions();
            Assert.assertEquals(2, epochExecutions.size());
            for (Map.Entry<Long, Set<UUID>> entry: epochExecutions.entrySet())
            {
                Assert.assertEquals(1, entry.getValue().size());
                Assert.assertTrue(expectedIds.containsAll(entry.getValue()));
                expectedIds.removeAll(entry.getValue());
            }
        }

        Assert.assertEquals(0, expectedIds.size());

        // check that the none of the excluded ids exist
        for (UUID id: excludedIds)
        {
            Assert.assertNull(toNode.getInstance(id));
        }
    }

    /**
     * Tests that both sides behave as expected
     * when the from-node cannot create a token
     * state required by the to-node
     */
    @Test
    public void missingRightTokenState()
    {

    }

    /**
     * Test that nothing breaks when draining the instances stream
     */
    @Test
    public void draining() throws Exception
    {
        UUID cfId = cfm.cfId;
        Node fromNode = nodes.get(0);
        Node toNode = nodes.get(1);

        Token token100 = partitioner.getToken(ByteBufferUtil.bytes(100));
        Token token200 = partitioner.getToken(ByteBufferUtil.bytes(200));

        TokenStateManager fTsm = fromNode.getTokenStateManager(DEFAULT_SCOPE);
        TokenStateManager.ManagedCf fCf = fTsm.getOrInitManagedCf(cfId);
        fCf.putIfAbsent(new TokenState(range(token100, token200), cfId, 10, 0));

        KeyStateManager fKsm = fromNode.getKeyStateManager(DEFAULT_SCOPE);

        for (int k=150; k<=200; k+=50)
        {
            long epoch = 10;

            ByteBuffer key = ByteBufferUtil.bytes(k);
            KeyState ks = fKsm.loadKeyState(key, cfId);
            TokenState ts = fTsm.get(key, cfId);

            Assert.assertEquals(String.format("Key: %s", k), epoch, ks.getEpoch());
            Assert.assertEquals(String.format("Key: %s", k), epoch, ts.getEpoch());

            // execute and instance in epoch, epoch -1, and epoch -2
            // and add an 'active' instance
            executeInstance(k, epoch - 2, fromNode, Scope.GLOBAL);
            executeInstance(k, epoch - 1, fromNode, Scope.GLOBAL);
            executeInstance(k, epoch, fromNode, Scope.GLOBAL);
            activeInstance(k, fromNode);
        }


        // set the to-node token state to the same epoch as the from-node
        TokenStateManager tTsm = toNode.getTokenStateManager(DEFAULT_SCOPE);
        TokenStateManager.ManagedCf tCf = tTsm.getOrInitManagedCf(cfId);
        tCf.putIfAbsent(new TokenState(range(token100, token200), cfId, 10, 0));

        final Range<Token> range = new Range<>(token100, token200);

        InstanceStreamWriter writer = new InstanceStreamWriter(fromNode, cfId, range, DEFAULT_SCOPE, toNode.getEndpoint());
        final AtomicBoolean wasDrained = new AtomicBoolean(false);
        InstanceStreamReader reader = new InstanceStreamReader(toNode, cfId, range, DEFAULT_SCOPE, fromNode.getEndpoint()) {
            @Override
            protected int drainInstanceStream(DataInputStream in) throws IOException
            {
                wasDrained.set(true);
                return super.drainInstanceStream(in);
            }
        };

        DataOutputBuffer outputBuffer = new DataOutputBuffer();
        writer.write(outputBuffer);

        ReadableByteChannel inputChannel = Channels.newChannel(new ByteArrayInputStream(outputBuffer.getData()));
        reader.read(inputChannel, null);

        Assert.assertTrue(wasDrained.get());
    }

    @Test
    public void tokenStatesAreIncremented()
    {

    }

    @Test
    public void illegalScopePeerCombo()
    {
        // doing stupid things is not allowed
        MockMultiDcService service = new MockMultiDcService();

        try
        {
            new InstanceStreamReader(service, CFID, null, Scope.LOCAL, REMOTE_ADDRESS);
            Assert.fail();
        }
        catch (AssertionError e)
        {
            // expected
        }

        try
        {
            new InstanceStreamWriter(service, CFID, null, Scope.LOCAL, REMOTE_ADDRESS);
            Assert.fail();
        }
        catch (AssertionError e)
        {
            // expected
        }
    }
}
