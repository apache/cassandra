package org.apache.cassandra.tools;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TopPartitionsTest
{
    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testServiceTopPartitionsNoArg() throws Exception
    {
        BlockingQueue<Map<String, Map<String, CompositeData>>> q = new ArrayBlockingQueue<>(1);
        ColumnFamilyStore.all();
        Executors.newCachedThreadPool().execute(() ->
        {
            try
            {
                q.put(StorageService.instance.samplePartitions(1000, 100, 10, Lists.newArrayList("READS", "WRITES")));
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        SystemKeyspace.persistLocalMetadata();
        Map<String, Map<String, CompositeData>> result = q.poll(11, TimeUnit.SECONDS);
        List<CompositeData> cd = (List<CompositeData>) (Object) Lists.newArrayList(((TabularDataSupport) result.get("system.local").get("WRITES").get("partitions")).values());
        assertEquals(1, cd.size());
    }

    @Test
    public void testServiceTopPartitionsSingleTable() throws Exception
    {
        ColumnFamilyStore.getIfExists("system", "local").beginLocalSampling("READS", 5);
        String req = "SELECT * FROM system.%s WHERE key='%s'";
        executeInternal(format(req, SystemKeyspace.LOCAL, SystemKeyspace.LOCAL));
        CompositeData result = ColumnFamilyStore.getIfExists("system", "local").finishLocalSampling("READS", 5);
        List<CompositeData> cd = (List<CompositeData>) (Object) Lists.newArrayList(((TabularDataSupport) result.get("partitions")).values());
        assertEquals(1, cd.size());
    }
}
