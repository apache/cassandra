package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.Random;

import javax.naming.ConfigurationException;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.schema.KeyspaceParams;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CommitLogSegmentManagerTest
{
    private static final String KEYSPACE1 = "CommitLogTest";
    private static final String STANDARD1 = "Standard1";
    private static final String STANDARD2 = "Standard2";

    private final static byte[] entropy = new byte[1024 * 256];
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        new Random().nextBytes(entropy);
        DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", ImmutableMap.of()));
        DatabaseDescriptor.setCommitLogSegmentSize(1);
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(10 * 1000);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD2, 0, AsciiType.instance, BytesType.instance));

        CompactionManager.instance.disableAutoCompaction();
    }

    @Test
    public void testCompressedCommitLogBackpressure() throws Throwable
    {
        CommitLog.instance.resetUnsafe(true);
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);

        Mutation m = new RowUpdateBuilder(cfs1.metadata, 0, "k")
                     .clustering("bytes")
                     .add("val", ByteBuffer.wrap(entropy))
                     .build();

        for (int i = 0; i < 20000; i++)
            CommitLog.instance.add(m);
    }
}
