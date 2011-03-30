package org.apache.cassandra.cql.jdbc;

import org.apache.cassandra.cql.EmbeddedServiceBase;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PreparedStatementTest extends EmbeddedServiceBase
{ 
    private static java.sql.Connection con = null;
    
    @BeforeClass
    public static void waxOn() throws Exception
    {
        startCassandraServer();
        Class.forName("org.apache.cassandra.cql.jdbc.CassandraDriver");
        con = DriverManager.getConnection("jdbc:cassandra:root/root@localhost:9170/Keyspace1");
    }
    
    @Test
    public void testBytes() throws SQLException
    {
        // insert
        PreparedStatement stmt = con.prepareStatement("update JdbcBytes set ?=?, ?=? where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setBytes(1, FBUtilities.toByteArray(i));
            stmt.setBytes(2, FBUtilities.toByteArray((i+1)*10));
            stmt.setBytes(3, FBUtilities.toByteArray(i+100));
            stmt.setBytes(4, FBUtilities.toByteArray((i+1)*10+1));
            stmt.setBytes(5, key);
            stmt.executeUpdate();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcBytes where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setBytes(1, FBUtilities.toByteArray(i));
            stmt.setBytes(2, FBUtilities.toByteArray(i+100));
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert rs.next();
            assert Arrays.equals(rs.getBytes(FBUtilities.bytesToHex(FBUtilities.toByteArray(i))), FBUtilities.toByteArray((i+1)*10));
            assert Arrays.equals(rs.getBytes(FBUtilities.bytesToHex(FBUtilities.toByteArray(i+100))), FBUtilities.toByteArray((i+1)*10+1));
            assert !rs.next();
            rs.close();
        }
        
        // delete
        stmt = con.prepareStatement("delete ?, ? from JdbcBytes where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setBytes(1, FBUtilities.toByteArray(i));
            stmt.setBytes(2, FBUtilities.toByteArray(i+100));
            stmt.setBytes(3, key);
            stmt.execute();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcBytes where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setBytes(1, FBUtilities.toByteArray(i));
            stmt.setBytes(2, FBUtilities.toByteArray(i+100));
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert !rs.next();
            rs.close();
        }
    }
    
    @Test
    public void testUtf8() throws SQLException
    {
        // insert.
        PreparedStatement stmt = con.prepareStatement("update JdbcUtf8 set ?=?, ?=? where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1\u6543\u3435\u6554");
            stmt.setString(2, "abc\u6543\u3435\u6554");
            stmt.setString(3, "2\u6543\u3435\u6554");
            stmt.setString(4, "def\u6543\u3435\u6554");
            stmt.setBytes(5, key);
            stmt.executeUpdate();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcUtf8 where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1\u6543\u3435\u6554");
            stmt.setString(2, "2\u6543\u3435\u6554");
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert rs.next();
            assert rs.getString("1\u6543\u3435\u6554").equals("abc\u6543\u3435\u6554");
            assert rs.getString("2\u6543\u3435\u6554").equals("def\u6543\u3435\u6554");
            assert !rs.next();
            rs.close();
        }
        
        // delete
        stmt = con.prepareStatement("delete ?, ? from JdbcUtf8 where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1\u6543\u3435\u6554");
            stmt.setString(2, "2\u6543\u3435\u6554");
            stmt.setBytes(3, key);
            stmt.execute();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcUtf8 where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1\u6543\u3435\u6554");
            stmt.setString(2, "2\u6543\u3435\u6554");
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert !rs.next();
            rs.close();
        }
    }
    
    @Test
    public void testAscii() throws SQLException
    {
        // insert.
        PreparedStatement stmt = con.prepareStatement("update JdbcAscii set ?=?, ?=? where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1");
            stmt.setString(2, "abc");
            stmt.setString(3, "2");
            stmt.setString(4, "def");
            stmt.setBytes(5, key);
            stmt.executeUpdate();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcAscii where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1");
            stmt.setString(2, "2");
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert rs.next();
            assert rs.getString("1").equals("abc");
            assert rs.getString("2").equals("def");
            assert !rs.next();
            rs.close();
        }
        
        // delete
        stmt = con.prepareStatement("delete ?, ? from JdbcAscii where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1");
            stmt.setString(2, "2");
            stmt.setBytes(3, key);
            stmt.execute();
        }
        
        // verify
        stmt = con.prepareStatement("select ?, ? from JdbcAscii where key=?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setString(1, "1");
            stmt.setString(2, "2");
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert !rs.next();
            rs.close();
        }
    }
    
    @Test
    public void testLong() throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement("update JdbcLong set ?=?, ?=? where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setLong(1, 1);
            stmt.setLong(2, (i+1)*10);
            stmt.setLong(3, 2);
            stmt.setLong(4, (i+1)*10+1);
            stmt.setBytes(5, key);
            stmt.executeUpdate();
        }
        stmt.close();
        
        // verify.
        stmt = con.prepareStatement("select ?, ? from JdbcLong where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setLong(1, 1);
            stmt.setLong(2, 2);
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert rs.next();
            assert rs.getLong("1") == (i+1)*10;
            assert rs.getLong("2") == (i+1)*10+1;
            assert !rs.next();
            rs.close();
        }
        
        // delete
        stmt = con.prepareStatement("delete ?, ? from JdbcLong where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setLong(1, 1);
            stmt.setLong(2, 2);
            stmt.setBytes(3, key);
            stmt.execute();
        }
        
        // verify.
        stmt = con.prepareStatement("select ?, ? from JdbcLong where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setLong(1, 1);
            stmt.setLong(2, 2);
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert !rs.next();
            rs.close();
        }
    }
    
    @Test
    public void testInteger() throws SQLException
    {
        PreparedStatement stmt = con.prepareStatement("update JdbcInteger set ?=?, ?=? where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setInt(1, 1);
            stmt.setInt(2, (i+1)*10);
            stmt.setInt(3, 2);
            stmt.setInt(4, (i+1)*10+1);
            stmt.setBytes(5, key);
            stmt.executeUpdate();
        }
        stmt.close();
        
        // verify.
        stmt = con.prepareStatement("select ?, ? from JdbcInteger where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert rs.next();
            assert rs.getInt("1") == (i+1)*10;
            assert rs.getInt("2") == (i+1)*10+1;
            assert !rs.next();
            rs.close();
        }
        
        // delete
        stmt = con.prepareStatement("delete ?, ? from JdbcInteger where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, key);
            stmt.execute();
        }
        
        // verify.
        stmt = con.prepareStatement("select ?, ? from JdbcInteger where key = ?");
        for (int i = 0; i < 5; i++)
        {
            byte[] key = Integer.toString(i).getBytes();
            stmt.setInt(1, 1);
            stmt.setInt(2, 2);
            stmt.setBytes(3, key);
            ResultSet rs = stmt.executeQuery();
            assert !rs.next();
            rs.close();
        }
    }
    
    @Test
    public void testParamSubstitution() throws SQLException
    {
        byte[] key = "key".getBytes();
        String q = "SELECT 'fo??est', ?, ? from JdbcUtf8 WHERE KEY = ?";
        CassandraPreparedStatement stmt = (CassandraPreparedStatement)con.prepareStatement(q);
        stmt.setString(1, "pathological param: ?'make it?? '' sto'p?'");
        stmt.setString(2, "simple");
        stmt.setBytes(3, key);
        String qq = stmt.makeCql();
        assert qq.equals("SELECT 'fo??est', 'pathological param: ?''make it?? '''' sto''p?''', 'simple' from JdbcUtf8 WHERE KEY = '6b6579'");
        
        q = "UPDATE JdbcUtf8 USING CONSISTENCY ONE SET 'fru??us'=?, ?='gr''d?', ?=?, ?=? WHERE key=?";
        stmt = (CassandraPreparedStatement)con.prepareStatement(q);
        stmt.setString(1, "o?e");
        stmt.setString(2, "tw'o");
        stmt.setString(3, "thr'?'ee");
        stmt.setString(4, "fo''?'ur");
        stmt.setString(5, "five");
        stmt.setString(6, "six");
        stmt.setBytes(7, key);
        qq = stmt.makeCql();
        assert qq.equals("UPDATE JdbcUtf8 USING CONSISTENCY ONE SET 'fru??us'='o?e', 'tw''o'='gr''d?', 'thr''?''ee'='fo''''?''ur', 'five'='six' WHERE key='6b6579'");
        
        q = "DELETE ?, ? FROM JdbcUtf8 WHERE KEY=?";
        stmt = (CassandraPreparedStatement)con.prepareStatement(q);
        stmt.setString(1, "on'?'");
        stmt.setString(2, "two");
        stmt.setBytes(3, key);
        qq = stmt.makeCql();
        assert qq.equals("DELETE 'on''?''', 'two' FROM JdbcUtf8 WHERE KEY='6b6579'");
    }
}
