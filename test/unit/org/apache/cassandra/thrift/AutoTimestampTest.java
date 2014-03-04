package org.apache.cassandra.thrift;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class AutoTimestampTest extends SchemaLoader
{
    private static CassandraServer server;
    private static final ColumnParent COLUMN_PARENT = new ColumnParent("Standard1");
    private static final String KEYSPACE = "keyspace1";
    
    @BeforeClass
    public static void setup() throws IOException, TException
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));
        server = new CassandraServer();
        server.set_keyspace(KEYSPACE);
    }    
    
    @Test
    public void autoTimestamps() throws InvalidRequestException, UnavailableException, TimedOutException, NotFoundException
    {
        Column column = new Column();
        column.setName(ByteBufferUtil.bytes("columnname"));
        column.setValue(ByteBufferUtil.bytes("columnvalue"));
        server.insert(ByteBufferUtil.bytes("myrow"), COLUMN_PARENT, column, ConsistencyLevel.ONE);
        ColumnPath cp = new ColumnPath();
        cp.setColumn_family("Standard1");
        cp.setColumn(ByteBufferUtil.bytes("columnname"));
        Column result = server.get(ByteBufferUtil.bytes("myrow"), cp , ConsistencyLevel.ONE).getColumn();
        Assert.assertTrue(result.getTimestamp() > 0);
    }

}
