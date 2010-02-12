package org.apache.cassandra.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

public class ColumnFamilyInputFormat extends InputFormat<String, SortedMap<byte[], IColumn>>
{
    private static final String KEYSPACE_CONFIG = "cassandra.input.keyspace";
    private static final String COLUMNFAMILY_CONFIG = "cassandra.input.columnfamily";

    private static final Logger logger = Logger.getLogger(StorageService.class);

    private String keyspace;
    private String columnFamily;

    public static void setColumnFamily(Job job, String keyspace, String columnFamily)
    {
        validateNotNullKeyspaceAndColumnFamily(keyspace, columnFamily);
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

    private static void validateNotNullKeyspaceAndColumnFamily(String keyspace, String columnFamily)
    {
        if (keyspace == null)
        {
            throw new RuntimeException("you forgot to set the keyspace with setKeyspace()");
        }
        if (columnFamily == null)
        {
            throw new RuntimeException("you forgot to set the column family with setColumnFamily()");
        }
    }

    public List<InputSplit> getSplits(JobContext context) throws IOException
    {
        Configuration conf = context.getConfiguration();
        keyspace = conf.get(KEYSPACE_CONFIG);
        columnFamily = conf.get(COLUMNFAMILY_CONFIG);
        validateNotNullKeyspaceAndColumnFamily(keyspace, columnFamily);

        List<TokenRange> map = getRangeMap();
        ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
        for (TokenRange entry : map)
        {
            if (logger.isDebugEnabled())
                logger.debug("split range is [" + entry.start_token + ", " + entry.end_token + "]");
            String[] endpoints = entry.endpoints.toArray(new String[0]);
            splits.add(new ColumnFamilySplit(keyspace, columnFamily, entry.start_token, entry.end_token, endpoints));
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