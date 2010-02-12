package org.apache.cassandra.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyInputFormat extends InputFormat<String, SortedMap<byte[], IColumn>>
{
    private static final String KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";
    private static final String PREDICATE_CONFIG = "cassandra.input.predicate";

    private static final Logger logger = Logger.getLogger(StorageService.class);

    private String keyspace;
    private String columnFamily;
    private SlicePredicate predicate;

    public static void setColumnFamily(Job job, String keyspace, String columnFamily)
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
        Configuration conf = job.getConfiguration();
        conf.set(KEYSPACE_CONFIG, keyspace);
        conf.set(COLUMNFAMILY_CONFIG, columnFamily);
    }

    public static void setSlicePredicate(Job job, SlicePredicate predicate)
    {
        Configuration conf = job.getConfiguration();
        conf.set(PREDICATE_CONFIG, predicateToString(predicate));
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

    private SlicePredicate predicateFromString(String st)
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

    private void validateConfiguration()
    {
        if (keyspace == null || columnFamily == null)
        {
            throw new UnsupportedOperationException("you must set the keyspace and columnfamily with setColumnFamily()");
        }
        if (predicate == null)
        {
            throw new UnsupportedOperationException("you must set the predicate with setPredicate");
        }
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();
        keyspace = conf.get(KEYSPACE_CONFIG);
        columnFamily = conf.get(COLUMNFAMILY_CONFIG);
        predicate = predicateFromString(conf.get(PREDICATE_CONFIG));
        validateConfiguration();

        List<TokenRange> map = getRangeMap();
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (TokenRange entry : map)
        {
            if (logger.isDebugEnabled())
                logger.debug("split range is [" + entry.start_token + ", " + entry.end_token + "]");
            String[] endpoints = entry.endpoints.toArray(new String[0]);
            splits.add(new ColumnFamilySplit(keyspace, columnFamily, predicate, entry.start_token, entry.end_token, endpoints));
        }

        return splits;
    }

    private List<TokenRange> getRangeMap() throws IOException
    {
        TSocket socket = new TSocket(DatabaseDescriptor.getSeeds().iterator().next().getHostAddress(),
                                     DatabaseDescriptor.getThriftPort());
        TBinaryProtocol binaryProtocol = new TBinaryProtocol(socket, false, false);
        Cassandra.Client client = new Cassandra.Client(binaryProtocol);
        try
        {
            socket.open();
        }
        catch (TTransportException e)
        {
            throw new IOException(e);
        }
        List<TokenRange> map;
        try
        {
            map = client.describe_ring(keyspace);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        return map;
    }

    @Override
    public RecordReader<String, SortedMap<byte[], IColumn>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException
    {
        return new ColumnFamilyRecordReader();
    }
}