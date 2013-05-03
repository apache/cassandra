package org.apache.cassandra.io.sstable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.junit.Test;

import com.google.common.collect.Lists;

public class IndexSummaryTest
{
    @Test
    public void testGetKey()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            Assert.assertEquals(random.left.get(i).key, ByteBuffer.wrap(random.right.getKey(i)));
    }

    @Test
    public void testBinarySearch()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        for (int i = 0; i < 100; i++)
            Assert.assertEquals(i, random.right.binarySearch(random.left.get(i)));
    }

    @Test
    public void testGetPosition()
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 2);
        for (int i = 0; i < 50; i++)
            Assert.assertEquals(i*2, random.right.getPosition(i));
    }

    @Test
    public void testSerialization() throws IOException
    {
        Pair<List<DecoratedKey>, IndexSummary> random = generateRandomIndex(100, 1);
        ByteArrayOutputStream aos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(aos);
        IndexSummary.serializer.serialize(random.right, dos);
        // write junk
        dos.writeUTF("JUNK");
        dos.writeUTF("JUNK");
        FileUtils.closeQuietly(dos);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(aos.toByteArray()));
        IndexSummary is = IndexSummary.serializer.deserialize(dis, DatabaseDescriptor.getPartitioner());
        for (int i = 0; i < 100; i++)
            Assert.assertEquals(i, is.binarySearch(random.left.get(i)));
        // read the junk
        Assert.assertEquals(dis.readUTF(), "JUNK");
        Assert.assertEquals(dis.readUTF(), "JUNK");
        FileUtils.closeQuietly(dis);
    }

    private Pair<List<DecoratedKey>, IndexSummary> generateRandomIndex(int size, int interval)
    {
        List<DecoratedKey> list = Lists.newArrayList();
        IndexSummaryBuilder builder = new IndexSummaryBuilder(list.size(), interval);
        for (int i = 0; i < size; i++)
        {
            UUID uuid = UUID.randomUUID();
            DecoratedKey key = DatabaseDescriptor.getPartitioner().decorateKey(ByteBufferUtil.bytes(uuid));
            list.add(key);
        }
        Collections.sort(list);
        for (int i = 0; i < size; i++)
            builder.maybeAddEntry(list.get(i), i);
        IndexSummary summary = builder.build(DatabaseDescriptor.getPartitioner());
        return Pair.create(list, summary);
    }
}
