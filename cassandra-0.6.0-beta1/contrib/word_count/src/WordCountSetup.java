import java.util.Arrays;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.*;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class WordCountSetup
{
    private static final Logger logger = Logger.getLogger(WordCountSetup.class);

    public static final int TEST_COUNT = 4;

    public static void main(String[] args) throws Exception
    {
        StorageService.instance.initClient();
        logger.info("Sleeping " + WordCount.RING_DELAY);
        Thread.sleep(WordCount.RING_DELAY);
        assert !StorageService.instance.getLiveNodes().isEmpty();

        RowMutation rm;
        ColumnFamily cf;
        byte[] columnName;

        // text0: no rows

        // text1: 1 row, 1 word
        columnName = "text1".getBytes();
        rm = new RowMutation(WordCount.KEYSPACE, "Key0");
        cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
        cf.addColumn(new Column(columnName, "word1".getBytes(), 0));
        rm.add(cf);
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        logger.info("added text1");

        // text2: 1 row, 2 words
        columnName = "text2".getBytes();
        rm = new RowMutation(WordCount.KEYSPACE, "Key0");
        cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
        cf.addColumn(new Column(columnName, "word1 word2".getBytes(), 0));
        rm.add(cf);
        StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        logger.info("added text2");

        // text3: 1000 rows, 1 word
        columnName = "text3".getBytes();
        for (int i = 0; i < 1000; i++)
        {
            rm = new RowMutation(WordCount.KEYSPACE, "Key" + i);
            cf = ColumnFamily.create(WordCount.KEYSPACE, WordCount.COLUMN_FAMILY);
            cf.addColumn(new Column(columnName, "word1".getBytes(), 0));
            rm.add(cf);
            StorageProxy.mutateBlocking(Arrays.asList(rm), ConsistencyLevel.ONE);
        }
        logger.info("added text3");

        System.exit(0);
    }
}
