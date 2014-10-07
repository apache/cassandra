package org.apache.cassandra.service.epaxos;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.UUIDGen;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class EpaxosKeyStateTest
{
    private static final Set<UUID> EMPTY = Sets.newHashSet();
    private static final ReplayPosition REPLAY_POS = new ReplayPosition(1, 2);
    private static final long MAX_TIMESTAMP = 0;

    /**
     * Tests that dependencies are evicted once they've
     * been executed and acknowledged
     */
    @Test
    public void eviction() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        Set<UUID> expected = Sets.newHashSet();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));

        expected.add(dep0);
        UUID dep1 = UUIDGen.getTimeUUID();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep1));

        UUID dep2 = UUIDGen.getTimeUUID();
        dm.markAcknowledged(Sets.newHashSet(dep0), dep1);
        Assert.assertEquals(1, dm.get(dep0).acknowledged.size());
        expected.add(dep1);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep2));

        UUID dep3 = UUIDGen.getTimeUUID();
        dm.markExecuted(dep0, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertNull(dm.get(dep0));

        expected.add(dep2);
        expected.remove(dep0);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep3));

        UUID dep4 = UUIDGen.getTimeUUID();
        expected.remove(dep0);
        expected.add(dep3);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep4));
    }

    @Test
    public void selfExcludedFromStronglyConnected() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        UUID dep1 = UUIDGen.getTimeUUID();

        KeyState.Entry entry = dm.recordInstance(dep0);
        dm.markExecuted(dep0, Sets.newHashSet(dep0, dep1), REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(Sets.newHashSet(dep1), entry.stronglyConnected);
    }

    /**
     * An acknowledgement from an instance in the same strongly connected
     * component should only prevent eviction if the instance being acknowledged
     * would be the last instance to be executed in that component;
     */
    @Test
    public void stronglyConnectedComponentEviction() throws Exception
    {
        KeyState dm = new KeyState(0);

        List<UUID> ids = Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID(),
                                            UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());

        // record, acknowledge, and execute every instance
        // as part of the same strongly connected component
        for (UUID id: ids)
        {
            dm.recordInstance(id);
            Assert.assertNotNull(dm.get(id));
            dm.markExecuted(id, Sets.newHashSet(ids), REPLAY_POS, MAX_TIMESTAMP);
            dm.markAcknowledged(Sets.newHashSet(ids), id);
        }

        Assert.assertNull(dm.get(ids.get(0)));
        Assert.assertNull(dm.get(ids.get(1)));
        Assert.assertNull(dm.get(ids.get(2)));
        Assert.assertNotNull(dm.get(ids.get(3)));
    }

    @Test
    public void ackFromNonSccTriggersEviction() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        dm.recordInstance(dep0);
        Assert.assertNotNull(dm.get(dep0));

        dm.markExecuted(dep0, Sets.newHashSet(dep0, dep1), REPLAY_POS, MAX_TIMESTAMP);
        dm.markAcknowledged(Sets.newHashSet(dep0), dep2);

        Assert.assertNull(dm.get(dep0));
    }

    /**
     * Tests that acknowledgements of instances the key
     * state doesn't know about yet don't disappear
     */
    @Test
    public void preAcksArentLost() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        dm.recordInstance(dep0);
        dm.markAcknowledged(Sets.newHashSet(dep0, dep1), dep2);
        dm.recordInstance(dep1);

        // check that the acknowledgement was recorded after the fact
        KeyState.Entry entry = dm.get(dep1);
        Assert.assertNotNull(entry);
        Assert.assertEquals(1, entry.acknowledged.size());
        Assert.assertTrue(entry.acknowledged.contains(dep2));

        dm.markExecuted(dep0, EMPTY, null, MAX_TIMESTAMP);
        dm.markExecuted(dep1, EMPTY, REPLAY_POS, MAX_TIMESTAMP);

        Assert.assertNull(dm.get(dep0));
        Assert.assertNull(dm.get(dep1));
    }

    @Test
    public void pendingAcksAreGcdOnEpochChange() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();

        dm.markAcknowledged(Sets.newHashSet(dep0, dep1), dep2);
        dm.removeEpoch(0l);

        dm.recordInstance(dep1);

        // check that no acknowledgement was recorded
        KeyState.Entry entry = dm.get(dep1);
        Assert.assertNotNull(entry);
        Assert.assertEquals(0, entry.acknowledged.size());
    }

    /**
     * Tests instances aren't assigned themselves as dependencies
     */
    @Test
    public void selfDependencies() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep0 = UUIDGen.getTimeUUID();
        Set<UUID> expected = Sets.newHashSet();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));

        expected.add(dep0);
        UUID dep1 = UUIDGen.getTimeUUID();
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep1));

        expected.add(dep1);
        expected.remove(dep0);
        Assert.assertEquals(expected, dm.getDepsAndAdd(dep0));
    }

    @Test
    public void executionCountIncrementedOnMarkExecuted() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep = UUIDGen.getTimeUUID();
        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(1, dm.getExecutionCount());
    }

    @Test
    public void executionCountResetOnEpochChange() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep = UUIDGen.getTimeUUID();

        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(1, dm.getExecutionCount());

        dm.setEpoch(1);
        Assert.assertEquals(0, dm.getExecutionCount());
    }

    @Test(expected=IllegalArgumentException.class)
    public void epochDecrementFailure() throws Exception
    {
        KeyState dm = new KeyState(1);
        Assert.assertEquals(1, dm.getEpoch());
        dm.setEpoch(0);
    }

    @Test
    public void executedInstanceBucketedByEpoch() throws Exception
    {
        KeyState dm = new KeyState(0);

        Assert.assertEquals(0, dm.getEpoch());

        UUID dep1 = UUIDGen.getTimeUUID();
        UUID dep2 = UUIDGen.getTimeUUID();
        UUID dep3 = UUIDGen.getTimeUUID();

        dm.setEpoch(1);
        dm.markExecuted(dep1, EMPTY, REPLAY_POS, MAX_TIMESTAMP);

        dm.setEpoch(2);
        dm.markExecuted(dep2, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        dm.markExecuted(dep3, EMPTY, REPLAY_POS, MAX_TIMESTAMP);

        Map<Long, Set<UUID>> executions = dm.getEpochExecutions();
        Assert.assertNull(executions.get((long) 0));
        Assert.assertEquals(Sets.newHashSet(dep1), executions.get((long) 1));
        Assert.assertEquals(Sets.newHashSet(dep2, dep3), executions.get((long) 2));
    }

    @Test
    public void epochSetIsIdempotent() throws Exception
    {
        KeyState dm = new KeyState(1, 2);

        Assert.assertEquals(2, dm.getExecutionCount());

        UUID dep = UUIDGen.getTimeUUID();
        dm.markExecuted(dep, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(3, dm.getExecutionCount());
        dm.markExecuted(dep, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(3, dm.getExecutionCount());
    }

    @Test
    public void markExecutedIdempotent() throws Exception
    {
        KeyState dm = new KeyState(0);

        UUID dep = UUIDGen.getTimeUUID();
        dm.getDepsAndAdd(dep);

        Assert.assertEquals(0, dm.getEpoch());
        Assert.assertEquals(0, dm.getExecutionCount());

        dm.markExecuted(dep, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
        Assert.assertEquals(1, dm.getExecutionCount());
    }

    @Test
    public void getEpochsOlderThanSuccess() throws Exception
    {
        Map<Long, Set<UUID>> expectedEpochs = new HashMap<>();
        long targetEpoch = 4;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            Set<UUID> ids = new HashSet<>();
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
                ids.add(id);
            }
            assert keyState.getExecutionCount() == 2;

            // add to expected if we're below the end epoch
            if (i < targetEpoch - 2)
            {
                expectedEpochs.put(i, ids);
            }
        }

        keyState.setEpoch(targetEpoch);
        Map<Long, Set<UUID>> actualEpochs = keyState.getEpochsOlderThan(targetEpoch - 2);
        Assert.assertEquals(expectedEpochs, actualEpochs);
    }

    @Test(expected=IllegalArgumentException.class)
    public void getEpochsOlderThanFailure() throws Exception
    {
        long targetEpoch = 4;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
            }
            assert keyState.getExecutionCount() == 2;
        }

        keyState.setEpoch(targetEpoch);
        keyState.getEpochsOlderThan(targetEpoch - 1);
    }

    @Test
    public void canIncrementToEpochTrue() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            assert keyState.getEpoch() == i;
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
                if (i == currentEpoch)
                {
                    // if this is the 'current' or previous epoch, set the dependency as active
                    keyState.recordInstance(id);
                }
            }
            assert keyState.getExecutionCount() == 2;
        }

        Assert.assertTrue(keyState.canIncrementToEpoch(targetEpoch));
    }

    @Test
    public void canIncrementToEpochFalse() throws Exception
    {
        long targetEpoch = 4;
        long currentEpoch = targetEpoch - 1;

        KeyState keyState = new KeyState(0);
        for (long i=0; i<targetEpoch; i++)
        {
            keyState.setEpoch(i);
            for (UUID id: Lists.newArrayList(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID()))
            {
                keyState.markExecuted(id, EMPTY, REPLAY_POS, MAX_TIMESTAMP);
                if (i == currentEpoch - 1)
                {
                    // if this is the 'current' epoch, set the dependency as active
                    keyState.recordInstance(id);
                }
            }
        }

        Assert.assertFalse(keyState.canIncrementToEpoch(targetEpoch));
    }

    /**
     * Checks that the correct epoch/exec# can be determined from a replay position
     */
    @Test
    public void getExecInfoFromReplayPosition() throws Exception
    {
        KeyState dm = new KeyState(0);

        Assert.assertEquals(0, dm.getEpoch());

        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, new ReplayPosition(0, 1), MAX_TIMESTAMP);

        dm.setEpoch(1);
        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, null, MAX_TIMESTAMP);

        dm.setEpoch(2);
        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, new ReplayPosition(1, 0), MAX_TIMESTAMP);
        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, new ReplayPosition(1, 5), MAX_TIMESTAMP);

        Assert.assertEquals(new ExecutionInfo(0, 0), dm.getExecutionInfoAtPosition(new ReplayPosition(0, 0)));
        Assert.assertEquals(new ExecutionInfo(0, 1), dm.getExecutionInfoAtPosition(new ReplayPosition(0, 1)));
        Assert.assertEquals(new ExecutionInfo(0, 1), dm.getExecutionInfoAtPosition(new ReplayPosition(0, 199)));
        Assert.assertEquals(new ExecutionInfo(2, 1), dm.getExecutionInfoAtPosition(new ReplayPosition(1, 0)));
        Assert.assertEquals(new ExecutionInfo(2, 1), dm.getExecutionInfoAtPosition(new ReplayPosition(1, 1)));
        Assert.assertEquals(new ExecutionInfo(2, 2), dm.getExecutionInfoAtPosition(new ReplayPosition(1, 10)));
    }

    @Test
    public void canExecute()
    {
        KeyState dm = new KeyState(0);
        Assert.assertTrue(dm.canExecute());

        Assert.assertTrue(dm.setFutureExecution(new ExecutionInfo(2, 2)));
        Assert.assertFalse(dm.canExecute());

        dm.setEpoch(2);
        Assert.assertEquals(0, dm.getExecutionCount());
        Assert.assertFalse(dm.canExecute());

        // this execution info should be rejected, since it's before the first one
        Assert.assertFalse(dm.setFutureExecution(new ExecutionInfo(1, 2)));

        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, null, MAX_TIMESTAMP);
        Assert.assertEquals(1, dm.getExecutionCount());
        Assert.assertFalse(dm.canExecute());

        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, null, MAX_TIMESTAMP);
        Assert.assertEquals(2, dm.getExecutionCount());
        Assert.assertTrue(dm.canExecute());

        dm.markExecuted(UUIDGen.getTimeUUID(), EMPTY, null, MAX_TIMESTAMP);
        Assert.assertEquals(3, dm.getExecutionCount());
        Assert.assertTrue(dm.canExecute());
    }

    @Test
    public void serialization() throws IOException
    {
        KeyState ks = new KeyState(5, 10);

        UUID id1 = UUIDGen.getTimeUUID();
        UUID id2 = UUIDGen.getTimeUUID();
        UUID id3 = UUIDGen.getTimeUUID();
        UUID id4 = UUIDGen.getTimeUUID();
        UUID id5 = UUIDGen.getTimeUUID();

        ks.recordInstance(id1);
        ks.markAcknowledged(Sets.newHashSet(id1), id2);
        ks.markAcknowledged(Sets.newHashSet(id3), id4);
        ks.recordInstance(id5);

        DataOutputBuffer out = new DataOutputBuffer();
        KeyState.serializer.serialize(ks, out, 0);

        long expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, KeyState.serializer.serializedSize(ks, 0));

        KeyState deserialized = KeyState.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
        Assert.assertEquals(ks, deserialized);
    }

    @Test
    public void entrySerialization() throws Exception
    {
        Set<UUID> stronglyConnected = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        Set<UUID> acknowledged = Sets.newHashSet(UUIDGen.getTimeUUID(), UUIDGen.getTimeUUID());
        KeyState.Entry entry = new KeyState.Entry(UUIDGen.getTimeUUID(), acknowledged, true);
        entry.setStronglyConnected(stronglyConnected);

        DataOutputBuffer out = new DataOutputBuffer();
        KeyState.Entry.serializer.serialize(entry, out, 0);

        long expectedSize = out.getLength();
        Assert.assertEquals(expectedSize, KeyState.Entry.serializer.serializedSize(entry, 0));

        KeyState.Entry deserialized = KeyState.Entry.serializer.deserialize(ByteStreams.newDataInput(out.getData()), 0);
        Assert.assertEquals(entry, deserialized);
    }

    @Test
    public void markQueryExecution()
    {
        KeyState ks = new KeyState(0);
        Assert.assertNull(ks.getLastQueryExecution());

        ks.setExecutionCount(5);
        ks.markQueryExecution();
        Assert.assertEquals(new ExecutionInfo(0, 5), ks.getLastQueryExecution());

        ks.setEpoch(3);
        Assert.assertEquals(new ExecutionInfo(0, 5), ks.getLastQueryExecution());

        ks.markQueryExecution();
        Assert.assertEquals(new ExecutionInfo(3, 0), ks.getLastQueryExecution());
    }

    @Test
    public void canGc() throws Exception
    {
        KeyState ks = new KeyState(0);
        Set<UUID> expectedDeps = new HashSet<>();

        UUID queryId = UUIDGen.getTimeUUID();
        UUID epochId1 = UUIDGen.getTimeUUID();
        UUID epochId2 = UUIDGen.getTimeUUID();

        for (UUID id: new UUID[]{queryId, epochId1, epochId2})
        {
            ks.getDepsAndAdd(id);
            expectedDeps.add(id);
        }

        ks.markQueryExecution();
        Assert.assertEquals(expectedDeps, ks.getActiveInstanceIds());
        Assert.assertEquals(new ExecutionInfo(0, 0), ks.getLastQueryExecution());
        Assert.assertFalse(ks.canGc(expectedDeps));

        ks.setEpoch(1);
        Assert.assertFalse(ks.canGc(expectedDeps));

        ks.setEpoch(2);
        Assert.assertTrue(ks.canGc(expectedDeps));
    }
}

