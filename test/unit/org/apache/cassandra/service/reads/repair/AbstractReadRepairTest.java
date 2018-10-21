package org.apache.cassandra.service.reads.repair;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.cassandra.locator.ReplicaPlan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.SingletonUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.ReplicaUtils.FULL_RANGE;

@Ignore
public abstract  class AbstractReadRepairTest
{
    static Keyspace ks;
    static ColumnFamilyStore cfs;
    static TableMetadata cfm;
    static InetAddressAndPort target1;
    static InetAddressAndPort target2;
    static InetAddressAndPort target3;
    static List<InetAddressAndPort> targets;

    static Replica replica1;
    static Replica replica2;
    static Replica replica3;
    static EndpointsForRange replicas;
    static ReplicaPlan.ForRead<?> replicaPlan;

    static long now = TimeUnit.NANOSECONDS.toMicros(System.nanoTime());
    static DecoratedKey key;
    static Cell cell1;
    static Cell cell2;
    static Cell cell3;
    static Mutation resolved;

    static ReadCommand command;

    static void assertRowsEqual(Row expected, Row actual)
    {
        try
        {
            Assert.assertEquals(expected == null, actual == null);
            if (expected == null)
                return;
            Assert.assertEquals(expected.clustering(), actual.clustering());
            Assert.assertEquals(expected.deletion(), actual.deletion());
            Assert.assertArrayEquals(Iterables.toArray(expected.cells(), Cell.class), Iterables.toArray(expected.cells(), Cell.class));
        } catch (Throwable t)
        {
            throw new AssertionError(String.format("Row comparison failed, expected %s got %s", expected, actual), t);
        }
    }

    static void assertRowsEqual(RowIterator expected, RowIterator actual)
    {
        assertRowsEqual(expected.staticRow(), actual.staticRow());
        while (expected.hasNext())
        {
            assert actual.hasNext();
            assertRowsEqual(expected.next(), actual.next());
        }
        assert !actual.hasNext();
    }

    static void assertPartitionsEqual(PartitionIterator expected, PartitionIterator actual)
    {
        while (expected.hasNext())
        {
            assert actual.hasNext();
            assertRowsEqual(expected.next(), actual.next());
        }

        assert !actual.hasNext();
    }

    static void assertMutationEqual(Mutation expected, Mutation actual)
    {
        Assert.assertEquals(expected.getKeyspaceName(), actual.getKeyspaceName());
        Assert.assertEquals(expected.key(), actual.key());
        Assert.assertEquals(expected.key(), actual.key());
        PartitionUpdate expectedUpdate = Iterables.getOnlyElement(expected.getPartitionUpdates());
        PartitionUpdate actualUpdate = Iterables.getOnlyElement(actual.getPartitionUpdates());
        assertRowsEqual(Iterables.getOnlyElement(expectedUpdate), Iterables.getOnlyElement(actualUpdate));
    }

    static DecoratedKey dk(int v)
    {
        return DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(v));
    }

    static Cell cell(String name, String value, long timestamp)
    {
        return BufferCell.live(cfm.getColumn(ColumnIdentifier.getInterned(name, false)), timestamp, ByteBufferUtil.bytes(value));
    }

    static PartitionUpdate update(Cell... cells)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        for (Cell cell: cells)
        {
            builder.addCell(cell);
        }
        return PartitionUpdate.singleRowUpdate(cfm, key, builder.build());
    }

    static PartitionIterator partition(Cell... cells)
    {
        UnfilteredPartitionIterator iter = new SingletonUnfilteredPartitionIterator(update(cells).unfilteredIterator());
        return UnfilteredPartitionIterators.filter(iter, Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(now)));
    }

    static Mutation mutation(Cell... cells)
    {
        return new Mutation(update(cells));
    }

    @SuppressWarnings("resource")
    static MessageIn<ReadResponse> msg(InetAddressAndPort from, Cell... cells)
    {
        UnfilteredPartitionIterator iter = new SingletonUnfilteredPartitionIterator(update(cells).unfilteredIterator());
        return MessageIn.create(from,
                                ReadResponse.createDataResponse(iter, command),
                                Collections.emptyMap(),
                                MessagingService.Verb.INTERNAL_RESPONSE,
                                MessagingService.current_version);
    }

    static class ResultConsumer implements Consumer<PartitionIterator>
    {

        PartitionIterator result = null;

        @Override
        public void accept(PartitionIterator partitionIterator)
        {
            Assert.assertNotNull(partitionIterator);
            result = partitionIterator;
        }
    }

    private static boolean configured = false;

    static void configureClass(ReadRepairStrategy repairStrategy) throws Throwable
    {
        SchemaLoader.loadSchema();
        String ksName = "ks";

        String ddl = String.format("CREATE TABLE tbl (k int primary key, v text) WITH read_repair='%s'",
                                   repairStrategy.toString().toLowerCase());

        cfm = CreateTableStatement.parse(ddl, ksName).build();
        assert cfm.params.readRepair == repairStrategy;
        KeyspaceMetadata ksm = KeyspaceMetadata.create(ksName, KeyspaceParams.simple(3), Tables.of(cfm));
        MigrationManager.announceNewKeyspace(ksm, false);

        ks = Keyspace.open(ksName);
        cfs = ks.getColumnFamilyStore("tbl");

        cfs.sampleReadLatencyNanos = 0;
        cfs.additionalWriteLatencyNanos = 0;

        target1 = InetAddressAndPort.getByName("127.0.0.255");
        target2 = InetAddressAndPort.getByName("127.0.0.254");
        target3 = InetAddressAndPort.getByName("127.0.0.253");

        targets = ImmutableList.of(target1, target2, target3);

        replica1 = fullReplica(target1, FULL_RANGE);
        replica2 = fullReplica(target2, FULL_RANGE);
        replica3 = fullReplica(target3, FULL_RANGE);
        replicas = EndpointsForRange.of(replica1, replica2, replica3);

        replicaPlan = replicaPlan(ConsistencyLevel.QUORUM, replicas);

        // default test values
        key  = dk(5);
        cell1 = cell("v", "val1", now);
        cell2 = cell("v", "val2", now);
        cell3 = cell("v", "val3", now);
        resolved = mutation(cell1, cell2);

        command = Util.cmd(cfs, 1).build();

        configured = true;
    }

    static Set<InetAddressAndPort> epSet(InetAddressAndPort... eps)
    {
        return Sets.newHashSet(eps);
    }

    @Before
    public void setUp()
    {
        assert configured : "configureClass must be called in a @BeforeClass method";

        cfs.sampleReadLatencyNanos = 0;
        cfs.additionalWriteLatencyNanos = 0;
    }

    static ReplicaPlan.ForRangeRead replicaPlan(ConsistencyLevel consistencyLevel, EndpointsForRange replicas)
    {
        return replicaPlan(ks, consistencyLevel, replicas, replicas);
    }

    static ReplicaPlan.ForRangeRead replicaPlan(EndpointsForRange replicas, EndpointsForRange targets)
    {
        return replicaPlan(ks, ConsistencyLevel.QUORUM, replicas, targets);
    }
    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForRange replicas)
    {
        return replicaPlan(keyspace, consistencyLevel, replicas, replicas);
    }
    static ReplicaPlan.ForRangeRead replicaPlan(Keyspace keyspace, ConsistencyLevel consistencyLevel, EndpointsForRange replicas, EndpointsForRange targets)
    {
        return new ReplicaPlan.ForRangeRead(keyspace, consistencyLevel,
                ReplicaUtils.FULL_BOUNDS, replicas, targets);
    }

    public abstract InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaPlan.Shared<?, ?> replicaPlan, long queryStartNanoTime);

    public InstrumentedReadRepair createInstrumentedReadRepair(ReplicaPlan.Shared<?, ?> replicaPlan)
    {
        return createInstrumentedReadRepair(command, replicaPlan, System.nanoTime());

    }

    /**
     * If we haven't received enough full data responses by the time the speculation
     * timeout occurs, we should send read requests to additional replicas
     */
    @Test
    public void readSpeculationCycle()
    {
        InstrumentedReadRepair repair = createInstrumentedReadRepair(ReplicaPlan.shared(replicaPlan(replicas, EndpointsForRange.of(replica1, replica2))));
        ResultConsumer consumer = new ResultConsumer();

        Assert.assertEquals(epSet(), repair.getReadRecipients());
        repair.startRepair(null, consumer);

        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());
        repair.maybeSendAdditionalReads();
        Assert.assertEquals(epSet(target1, target2, target3), repair.getReadRecipients());
        Assert.assertNull(consumer.result);
    }

    /**
     * If we receive enough data responses by the before the speculation timeout
     * passes, we shouldn't send additional read requests
     */
    @Test
    public void noSpeculationRequired()
    {
        InstrumentedReadRepair repair = createInstrumentedReadRepair(ReplicaPlan.shared(replicaPlan(replicas, EndpointsForRange.of(replica1, replica2))));
        ResultConsumer consumer = new ResultConsumer();

        Assert.assertEquals(epSet(), repair.getReadRecipients());
        repair.startRepair(null, consumer);

        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());
        repair.getReadCallback().response(msg(target1, cell1));
        repair.getReadCallback().response(msg(target2, cell1));

        repair.maybeSendAdditionalReads();
        Assert.assertEquals(epSet(target1, target2), repair.getReadRecipients());

        repair.awaitReads();

        assertPartitionsEqual(partition(cell1), consumer.result);
    }
}
