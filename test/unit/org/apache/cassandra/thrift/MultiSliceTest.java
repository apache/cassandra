package org.apache.cassandra.thrift;

import io.teknek.arizona.ArizonaServer;

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
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;
import org.junit.BeforeClass;
import org.junit.Test;

public class MultiSliceTest extends SchemaLoader
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

    private static MultiSliceRequest makeMultiSliceRequest(ByteBuffer key)
    {
        ColumnParent cp = new ColumnParent("Standard1");
        MultiSliceRequest req = new MultiSliceRequest();
        req.setKey(key);
        req.setCount(1000);
        req.reversed = false;
        req.setColumn_parent(cp);
        return req;
    }
    
    @Test
    public void test_multi_slice_optional_column_slice() throws TException
    {
      //az.func_modifify(null);
        ColumnParent cp = new ColumnParent("Standard1");
        ByteBuffer key = ByteBuffer.wrap("multi_slice".getBytes());
        List<String> expected = new ArrayList<String>();
        for (char a = 'a'; a <= 'z'; a++)
            expected.add(a + "");

        addTheAlphabetToRow(key, cp);
        MultiSliceRequest req = makeMultiSliceRequest(key);
        req.setColumn_slices(new ArrayList<ColumnSlice>());
        req.getColumn_slices().add(new ColumnSlice());
        List<ColumnOrSuperColumn> list = server.get_multi_slice(req);
        assertColumnNameMatches(expected, list);
    }
    
    @Test
    public void test_multi_slice() throws TException
    {
        ColumnParent cp = new ColumnParent("Standard1");
        ByteBuffer key = ByteBuffer.wrap("multi_slice_two_slice".getBytes());
        addTheAlphabetToRow(key, cp);
        MultiSliceRequest req = makeMultiSliceRequest(key);
        req.setColumn_slices(Arrays.asList(columnSliceFrom("a", "e"), columnSliceFrom("i", "n")));
        assertColumnNameMatches(Arrays.asList("a", "b", "c", "d", "e", "i", "j", "k" , "l", "m" , "n"), server.get_multi_slice(req));
    }
    
    @Test
    public void test_with_overlap() throws TException
    {
        ColumnParent cp = new ColumnParent("Standard1");
        ByteBuffer key = ByteBuffer.wrap("overlap".getBytes());
        addTheAlphabetToRow(key, cp);
        MultiSliceRequest req = makeMultiSliceRequest(key);
        req.setColumn_slices(Arrays.asList(columnSliceFrom("a", "e"), columnSliceFrom("d", "g")));
        assertColumnNameMatches(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), server.get_multi_slice(req));
    }
    
    @Test
    public void test_with_overlap_reversed() throws TException
    {
        ColumnParent cp = new ColumnParent("Standard1");
        ByteBuffer key = ByteBuffer.wrap("overlap_reversed".getBytes());
        addTheAlphabetToRow(key, cp);
        MultiSliceRequest req = makeMultiSliceRequest(key);
        req.reversed = true;
        req.setColumn_slices(Arrays.asList(columnSliceFrom("e", "a"), columnSliceFrom("g", "d")));
        assertColumnNameMatches(Arrays.asList("g", "f", "e", "d", "c", "b", "a"), server.get_multi_slice(req));
    }

    @Test(expected=InvalidRequestException.class)
    public void test_that_column_slice_is_proper() throws TException
    {
      ByteBuffer key = ByteBuffer.wrap("overlap".getBytes());
      MultiSliceRequest req = makeMultiSliceRequest(key);
      req.reversed = true;
      req.setColumn_slices(Arrays.asList(columnSliceFrom("a", "e"), columnSliceFrom("g", "d")));
      assertColumnNameMatches(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), server.get_multi_slice(req));
    }
    
    @Test
    public void test_with_overlap_reversed_with_count() throws TException
    {
        ColumnParent cp = new ColumnParent("Standard1");
        ByteBuffer key = ByteBuffer.wrap("overlap_reversed_count".getBytes());
        addTheAlphabetToRow(key, cp);
        MultiSliceRequest req = makeMultiSliceRequest(key);
        req.setCount(6);
        req.reversed = true;
        req.setColumn_slices(Arrays.asList(columnSliceFrom("e", "a"), columnSliceFrom("g", "d")));
        assertColumnNameMatches(Arrays.asList("g", "e", "d", "c", "b", "a"), server.get_multi_slice(req)); 
    }
    
    public static void addTheAlphabetToRow(ByteBuffer key, ColumnParent parent) 
            throws InvalidRequestException, UnavailableException, TimedOutException
    {
        for (char a = 'a'; a <= 'z'; a++) {
            Column c1 = new Column();
            c1.setName(ByteBufferUtil.bytes(String.valueOf(a)));
            c1.setValue(new byte [0]);
            c1.setTimestamp(System.nanoTime());
            server.insert(key, parent, c1, ConsistencyLevel.ONE); 
         }
    }
    
    private static void assertColumnNameMatches(List<String> expected , List<ColumnOrSuperColumn> actual)
    {
        Assert.assertEquals(actual+" "+expected +" did not have same number of elements", actual.size(), expected.size());
        for (int i = 0 ; i< expected.size() ; i++)
        {
            Assert.assertEquals(actual.get(i) +" did not equal "+ expected.get(i), 
                    new String(actual.get(i).getColumn().getName()), expected.get(i));
        }
    }
    
    private ColumnSlice columnSliceFrom(String startInclusive, String endInclusive)
    {
        ColumnSlice cs = new ColumnSlice();
        cs.setStart(ByteBufferUtil.bytes(startInclusive));
        cs.setFinish(ByteBufferUtil.bytes(endInclusive));
        return cs;
    }
}