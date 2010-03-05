package org.apache.cassandra.hadoop;

import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

public class ConfigHelper
{
    private static final String KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String PREDICATE_CONFIG = "cassandra.input.predicate";
    private static final String INPUT_SPLIT_SIZE_CONFIG = "cassandra.input.split.size";

    public static void setColumnFamily(Configuration conf, String keyspace, String columnFamily)
    {
        if (keyspace == null)
        {
            throw new UnsupportedOperationException("keyspace may not be null");
        }
        if (columnFamily == null)
        {
            throw new UnsupportedOperationException("columnfamily may not be null");
        }
        try
        {
            ThriftValidation.validateColumnFamily(keyspace, columnFamily);
        }
        catch (InvalidRequestException e)
        {
            throw new RuntimeException(e);
        }
        conf.set(KEYSPACE_CONFIG, keyspace);
        conf.set(COLUMNFAMILY_CONFIG, columnFamily);
    }

    /**
     * Set the size of the input split.
     * This affects the number of maps created, if the number is too small
     * the overhead of each map will take up the bulk of the job time.
     *
     * @param conf Job configuration you are about to run.
     * @param splitsize Size of the input split
     */
    public static void setInputSplitSize(Configuration conf, int splitsize)
    {
        conf.setInt(INPUT_SPLIT_SIZE_CONFIG, splitsize);
    }

    public static int getInputSplitSize(Configuration conf)
    {
        return conf.getInt(INPUT_SPLIT_SIZE_CONFIG, 4096);
    }

    public static void setSlicePredicate(Configuration conf, SlicePredicate predicate)
    {
        conf.set(PREDICATE_CONFIG, predicateToString(predicate));
    }

    public static SlicePredicate getSlicePredicate(Configuration conf)
    {
        return predicateFromString(conf.get(PREDICATE_CONFIG));
    }

    private static String predicateToString(SlicePredicate predicate)
    {
        assert predicate != null;
        // this is so awful it's kind of cool!
        TSerializer serializer = new TSerializer(new TJSONProtocol.Factory());
        try
        {
            return serializer.toString(predicate, "UTF-8");
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static SlicePredicate predicateFromString(String st)
    {
        assert st != null;
        TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());
        SlicePredicate predicate = new SlicePredicate();
        try
        {
            deserializer.deserialize(predicate, st, "UTF-8");
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return predicate;
    }

    public static String getKeyspace(Configuration conf)
    {
        return conf.get(KEYSPACE_CONFIG);
    }

    public static String getColumnFamily(Configuration conf)
    {
        return conf.get(COLUMNFAMILY_CONFIG);
    }
}
