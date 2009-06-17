package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.FileStruct;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.utils.BloomFilter;

public class SSTableTest extends CleanupHelper
{
    @Test
    public void testSingleWrite() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        // write test data
        ssTable = new SSTable(f.getParent(), f.getName(), new OrderPreservingPartitioner());
        BloomFilter bf = new BloomFilter(1000, 8);
        Random random = new Random();
        byte[] bytes = new byte[1024];
        random.nextBytes(bytes);

        String key = Integer.toString(1);
        ssTable.append(key, bytes);
        bf.add(key);
        ssTable.close(bf);

        // verify
        SSTable.indexMetadataMap_.clear(); // force reloading the index
        ssTable = new SSTable(f.getPath() + "-Data.db", new OrderPreservingPartitioner());
        FileStruct fs = new FileStruct(SequenceFile.bufferedReader(ssTable.dataFile_, 128 * 1024), new OrderPreservingPartitioner());
        fs.seekTo(key);
        int size = fs.getBufIn().readInt();
        byte[] bytes2 = new byte[size];
        fs.getBufIn().readFully(bytes2);
        assert Arrays.equals(bytes2, bytes);
    }

    @Test
    public void testManyWrites() throws IOException {
        File f = File.createTempFile("sstable", "");
        SSTable ssTable;

        TreeMap<String, byte[]> map = new TreeMap<String,byte[]>();
        for ( int i = 100; i < 1000; ++i )
        {
            map.put(Integer.toString(i), ("Avinash Lakshman is a good man: " + i).getBytes());
        }

        // write
        ssTable = new SSTable(f.getParent(), f.getName(), new OrderPreservingPartitioner());
        BloomFilter bf = new BloomFilter(1000, 8);
        for (String key: map.navigableKeySet())
        {
            ssTable.append(key, map.get(key));
        }
        ssTable.close(bf);

        // verify
        SSTable.indexMetadataMap_.clear(); // force reloading the index
        List<String> keys = new ArrayList(map.keySet());
        Collections.shuffle(keys);
        ssTable = new SSTable(f.getPath() + "-Data.db", new OrderPreservingPartitioner());
        FileStruct fs = new FileStruct(SequenceFile.bufferedReader(ssTable.dataFile_, 128 * 1024), new OrderPreservingPartitioner());
        for (String key : keys)
        {
            fs.seekTo(key);
            int size = fs.getBufIn().readInt();
            byte[] bytes2 = new byte[size];
            fs.getBufIn().readFully(bytes2);
            assert Arrays.equals(bytes2, map.get(key));
        }
    }
}
