package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class SinglePartitionSliceCommandTest
{
    private static final Logger logger = LoggerFactory.getLogger(SinglePartitionSliceCommandTest.class);

    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static CFMetaData cfm;
    private static ColumnDefinition v;
    private static ColumnDefinition s;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        cfm = CFMetaData.Builder.create(KEYSPACE, TABLE)
                                .addPartitionKey("k", UTF8Type.instance)
                                .addStaticColumn("s", UTF8Type.instance)
                                .addClusteringColumn("i", IntegerType.instance)
                                .addRegularColumn("v", UTF8Type.instance)
                                .build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        v = cfm.getColumnDefinition(new ColumnIdentifier("v", true));
        s = cfm.getColumnDefinition(new ColumnIdentifier("s", true));
    }

    @Before
    public void truncate()
    {
        Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE).truncateBlocking();
    }

    @Test
    public void staticColumnsAreFiltered() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k"));

        UntypedResultSet rows;

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s, i, v) VALUES ('k', 's', 0, 'v')");
        QueryProcessor.executeInternal("DELETE v FROM ks.tbl WHERE k='k' AND i=0");
        QueryProcessor.executeInternal("DELETE FROM ks.tbl WHERE k='k' AND i=0");
        rows = QueryProcessor.executeInternal("SELECT * FROM ks.tbl WHERE k='k' AND i=0");

        for (UntypedResultSet.Row row: rows)
        {
            logger.debug("Current: k={}, s={}, v={}", (row.has("k") ? row.getString("k") : null), (row.has("s") ? row.getString("s") : null), (row.has("v") ? row.getString("v") : null));
        }

        assert rows.isEmpty();

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(v));
        ByteBuffer zero = ByteBufferUtil.bytes(0);
        Slices slices = Slices.with(cfm.comparator, Slice.make(Slice.Bound.inclusiveStartOf(zero), Slice.Bound.inclusiveEndOf(zero)));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(slices, false);
        ReadCommand cmd = new SinglePartitionReadCommand(false, MessagingService.VERSION_30, true, cfm,
                                                          FBUtilities.nowInSeconds(),
                                                          columnFilter,
                                                          RowFilter.NONE,
                                                          DataLimits.NONE,
                                                          key,
                                                          sliceFilter);

        DataOutputBuffer out = new DataOutputBuffer((int) ReadCommand.legacyReadCommandSerializer.serializedSize(cmd, MessagingService.VERSION_21));
        ReadCommand.legacyReadCommandSerializer.serialize(cmd, out, MessagingService.VERSION_21);
        DataInputPlus in = new DataInputBuffer(out.buffer(), true);
        cmd = ReadCommand.legacyReadCommandSerializer.deserialize(in, MessagingService.VERSION_21);

        logger.debug("ReadCommand: {}", cmd);
        UnfilteredPartitionIterator partitionIterator = cmd.executeLocally(ReadExecutionController.empty());
        ReadResponse response = ReadResponse.createDataResponse(partitionIterator, cmd);

        logger.debug("creating response: {}", response);
        partitionIterator = response.makeIterator(cmd);
        assert partitionIterator.hasNext();
        UnfilteredRowIterator partition = partitionIterator.next();

        LegacyLayout.LegacyUnfilteredPartition rowIter = LegacyLayout.fromUnfilteredRowIterator(cmd, partition);
        Assert.assertEquals(Collections.emptyList(), rowIter.cells);
    }

    private void checkForS(UnfilteredPartitionIterator pi)
    {
        Assert.assertTrue(pi.toString(), pi.hasNext());
        UnfilteredRowIterator ri = pi.next();
        Assert.assertTrue(ri.columns().contains(s));
        Row staticRow = ri.staticRow();
        Iterator<Cell> cellIterator = staticRow.cells().iterator();
        Assert.assertTrue(staticRow.toString(cfm, true), cellIterator.hasNext());
        Cell cell = cellIterator.next();
        Assert.assertEquals(s, cell.column());
        Assert.assertEquals(ByteBufferUtil.bytesToHex(cell.value()), ByteBufferUtil.bytes("s"), cell.value());
        Assert.assertFalse(cellIterator.hasNext());
    }

    @Test
    public void staticColumnsAreReturned() throws IOException
    {
        DecoratedKey key = cfm.decorateKey(ByteBufferUtil.bytes("k1"));

        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k1', 's')");
        Assert.assertFalse(QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k1'").isEmpty());

        ColumnFilter columnFilter = ColumnFilter.selection(PartitionColumns.of(s));
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.NONE, false);
        ReadCommand cmd = new SinglePartitionReadCommand(false, MessagingService.VERSION_30, true, cfm,
                                                         FBUtilities.nowInSeconds(),
                                                         columnFilter,
                                                         RowFilter.NONE,
                                                         DataLimits.NONE,
                                                         key,
                                                         sliceFilter);

        // check raw iterator for static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            checkForS(pi);
        }

        ReadResponse response;
        DataOutputBuffer out;
        DataInputPlus in;
        ReadResponse dst;

        // check (de)serialized iterator for memtable static cell
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd);
        }

        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }

        // check (de)serialized iterator for sstable static cell
        Schema.instance.getColumnFamilyStoreInstance(cfm.cfId).forceBlockingFlush();
        try (ReadExecutionController executionController = cmd.executionController(); UnfilteredPartitionIterator pi = cmd.executeLocally(executionController))
        {
            response = ReadResponse.createDataResponse(pi, cmd);
        }
        out = new DataOutputBuffer((int) ReadResponse.serializer.serializedSize(response, MessagingService.VERSION_30));
        ReadResponse.serializer.serialize(response, out, MessagingService.VERSION_30);
        in = new DataInputBuffer(out.buffer(), true);
        dst = ReadResponse.serializer.deserialize(in, MessagingService.VERSION_30);
        try (UnfilteredPartitionIterator pi = dst.makeIterator(cmd))
        {
            checkForS(pi);
        }
    }
}
