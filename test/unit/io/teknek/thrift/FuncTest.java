package io.teknek.thrift;


import io.teknek.arizona.ArizonaServer;
import io.teknek.arizona.FunctionalModifyRequest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

public class FuncTest extends SchemaLoader
{

    private static CassandraServer server;
    private static ArizonaServer az;
    
    @BeforeClass
    public static void setup() throws IOException, TException 
    {
        Schema.instance.clear(); // Schema are now written on disk and will be reloaded
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));        
        server = new CassandraServer();
        az = new ArizonaServer();
        server.set_keyspace("Keyspace1");
    }
    
    @Test
    public void firstTry() throws TException
    {
      FunctionalModifyRequest request = new FunctionalModifyRequest();
      request.setColumn_family("Standard1");
      request.setSerial_consistency_level(ConsistencyLevel.ONE);
      request.setCommit_consistency_level(ConsistencyLevel.ONE);
      request.setFunction_name("append");
      request.setKey(ByteBufferUtil.bytes("row"));
      az.func_modifify(request);
    }

}