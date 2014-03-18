package io.teknek.thrift;


import io.teknek.arizona.ArizonaServer;
import io.teknek.arizona.FunctionalModifyRequest;
import io.teknek.arizona.FunctionalTransformRequest;
import io.teknek.arizona.FunctionalTransformResponse;
import io.teknek.arizona.TransformRequest;
import io.teknek.arizona.TransformResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.MultiSliceTest;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
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
    
    public static void addTheAlphabetToRow(ByteBuffer key, ColumnParent parent) 
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
        for (char a = 'a'; a <= 'z'; a++) {
            Column c1 = new Column();
            c1.setName(ByteBufferUtil.bytes(String.valueOf(a)));
            c1.setValue(new byte [0]);
            c1.setTimestamp(FBUtilities.timestampMicros());
            server.insert(key, parent, c1, ConsistencyLevel.ONE); 
         }
    }
        
    @Test
    public void firstTry() throws TException, CharacterCodingException
    {
      ColumnParent parent = new ColumnParent("Standard1");
      addTheAlphabetToRow(ByteBufferUtil.bytes("row"), parent);
      TransformRequest request = new TransformRequest();
      request.setColumn_family("Standard1");
      request.setSerial_consistency_level(ConsistencyLevel.SERIAL);
      request.setCommit_consistency_level(ConsistencyLevel.ONE);
      request.setFunction_name("simple_transformer");
      request.setKey(ByteBufferUtil.bytes("row"));
      request.setPredicate(new SlicePredicate());
      request.getPredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("a")));
      TransformResponse resp = az.transform(request);
      Assert.assertEquals(true, resp.success);
      Assert.assertEquals("0", ByteBufferUtil.string(resp.getCurrent_value().get(0).value));
      List<ColumnOrSuperColumn> results = server.get_slice(ByteBufferUtil.bytes("row"), parent,request.getPredicate(), ConsistencyLevel.ONE);
      Assert.assertEquals("0", ByteBufferUtil.string(results.get(0).column.value));
    }
    
    
    public static void addColumnsOneToTen(ByteBuffer key, ColumnParent parent) 
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
      
      int i = 1;
        for (char a = 'a' ; a <= 'j'; a++, i++) {
            Column c1 = new Column();
            c1.setName(ByteBufferUtil.bytes(String.valueOf(a)));
            c1.setValue(ByteBufferUtil.bytes(String.valueOf(i)));
            c1.setTimestamp(FBUtilities.timestampMicros());
            server.insert(key, parent, c1, ConsistencyLevel.ONE); 
         }
    }
    
    @Test
    public void increment() throws TException, CharacterCodingException
    {
      ColumnParent parent = new ColumnParent("Standard1");
      addColumnsOneToTen(ByteBufferUtil.bytes("row2"), parent);
      TransformRequest request = new TransformRequest();
      request.setColumn_family("Standard1");
      request.setSerial_consistency_level(ConsistencyLevel.SERIAL);
      request.setCommit_consistency_level(ConsistencyLevel.ONE);
      request.setFunction_name("increment");
      request.setKey(ByteBufferUtil.bytes("row2"));
      request.setPredicate(new SlicePredicate());
      request.getPredicate().setColumn_names(Arrays.asList(ByteBufferUtil.bytes("a")));
      TransformResponse resp = az.transform(request);
      Assert.assertEquals(true, resp.success);
      Assert.assertEquals("2", ByteBufferUtil.string(resp.getCurrent_value().get(0).value));
      List<ColumnOrSuperColumn> results = server.get_slice(ByteBufferUtil.bytes("row2"), parent,request.getPredicate(), ConsistencyLevel.ONE);
      Assert.assertEquals("2", ByteBufferUtil.string(results.get(0).column.value));
    }
    

}