package org.apache.cassandra.cql.driver.jdbc;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.FloatBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.cassandra.utils.FBUtilities;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test case for unit test of various methods of JDBC implementation.
 */
public class JdbcDriverTest extends EmbeddedServiceBase
{
    private static java.sql.Connection con = null;
    private static final String first = FBUtilities.bytesToHex("first".getBytes());
    private static final String firstrec = FBUtilities.bytesToHex("firstrec".getBytes());
    private static final String last = FBUtilities.bytesToHex("last".getBytes());
    private static final String lastrec = FBUtilities.bytesToHex("lastrec".getBytes());

    /** SetUp */
    @BeforeClass
    public static void startServer() throws Exception
    {
        startCassandraServer();
        Class.forName("org.apache.cassandra.cql.driver.jdbc.CassandraDriver");
        con = DriverManager.getConnection("jdbc:cassandra:root/root@localhost:9170/Keyspace1");
        String[] inserts = 
        {
//            String.format("UPDATE Standard1 SET \"%s\" = \"%s\", \"%s\" = \"%s\" WHERE KEY = \"jsmith\"", first, firstrec, last, lastrec),    
            "UPDATE JdbcInteger SET 1 = 11, 2 = 22 WHERE KEY = \"jsmith\"",
            "UPDATE JdbcInteger SET 3 = 33, 4 = 44 WHERE KEY = \"jsmith\"",
            "UPDATE JdbcLong SET 1L = 11L, 2L = 22L WHERE KEY = \"jsmith\"",
            "UPDATE JdbcAscii SET \"first\" = \"firstrec\", \"last\" = \"lastrec\" WHERE key = \"jsmith\"",
//            String.format("UPDATE JdbcBytes SET \"%s\" = \"%s\", \"%s\" = \"%s\" WHERE key = \"jsmith\"", first, firstrec, last, lastrec),
            "UPDATE JdbcUtf8 SET \"first\" = \"firstrec\", \"last\" = \"lastrec\" WHERE key = \"jsmith\"",
        };
        for (String q : inserts)
            executeNoResults(con, q);
    }
    
    /** Method to test statement. */
    @Test
    public void testWithStatement() throws SQLException
    {
        Statement stmt = con.createStatement();
        
//        String selectQ = String.format("SELECT \"%s\", \"%s\" FROM Standard1 WHERE KEY=\"jsmith\"", first, last);
//        checkResultSet(stmt.executeQuery(selectQ), "Bytes", 1, first, last);
        
        String selectQ = "SELECT 1, 2 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, "1", "2");
        
        selectQ = "SELECT 3, 4 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, "3", "4");
        
        selectQ = "SELECT 1, 2, 3, 4 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "Int", 1, "1", "2", "3", "4");
        
        selectQ = "SELECT 1L, 2L FROM JdbcLong WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "Long", 1, "1", "2");
        
        selectQ = "SELECT \"first\", \"last\" FROM JdbcAscii WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "String", 1, "first", "last");
        
//        selectQ = String.format("SELECT \"%s\", \"%s\" FROM JdbcBytes WHERE KEY=\"jsmith\"", first, last);
//        checkResultSet(stmt.executeQuery(selectQ), "Bytes", 1, first, last);
        
        selectQ = "SELECT \"first\", \"last\" FROM JdbcUtf8 WHERE KEY=\"jsmith\"";
        checkResultSet(stmt.executeQuery(selectQ), "String", 1, "first", "last");
    }

   /** Method to test with prepared statement.*/
   @Test
    public void testWithPreparedStatement() throws SQLException
    {
//        String selectQ = "SELECT \"first\", \"last\" FROM Standard1 WHERE KEY=\"jsmith\"";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, "first", "last");
        
        String selectQ = "SELECT 1, 2 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, "1", "2");
        
        selectQ = "SELECT 3, 4 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, "3", "4");
        
        selectQ = "SELECT 1, 2, 3, 4 FROM JdbcInteger WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Int", 1, "1", "2", "3", "4");
        
        selectQ = "SELECT 1L, 2L FROM JdbcLong WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Long", 1, "1", "2");
        
        selectQ = "SELECT \"first\", \"last\" FROM JdbcAscii WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "String", 1, "first", "last");
        
//        selectQ = "SELECT \"first\", \"last\" FROM JdbcBytes WHERE KEY=\"jsmith\"";
//        checkResultSet(executePreparedStatementWithResults(con, selectQ), "Bytes", 1, "first", "last");
        
        selectQ = "SELECT \"first\", \"last\" FROM JdbcUtf8 WHERE KEY=\"jsmith\"";
        checkResultSet(executePreparedStatementWithResults(con, selectQ), "String", 1, "first", "last");
    }

    /* Method to test with Delete statement. */
    @Test
    public void testWithDeleteStatement() throws SQLException
    {
        // the pattern: 0) a deltion, 1) ensure deletion 2) ensure deletion wasn't over-eager.
        String[] statements = 
        {
//                "DELETE \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"",
//                "SELECT \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"",
//                "SELECT \"first\" FROM Standard1 WHERE KEY=\"jsmith\"",
                
                "DELETE 1, 3 FROM JdbcInteger WHERE KEY=\"jsmith\"",
                "SELECT 1, 3 FROM JdbcInteger WHERE KEY=\"jsmith\"", // fails.
                "SELECT 2, 4 FROM JdbcInteger WHERE KEY=\"jsmith\"",
                
                "DELETE 1L FROM JdbcLong WHERE KEY=\"jsmith\"",
                "SELECT 1L FROM JdbcLong WHERE KEY=\"jsmith\"",
                "SELECT 2L FROM JdbcLong WHERE KEY=\"jsmith\"",
                
                "DELETE \"first\" FROM JdbcAscii WHERE KEY=\"jsmith\"",
                "SELECT \"first\" FROM JdbcAscii WHERE KEY=\"jsmith\"",
                "SELECT \"last\" FROM JdbcAscii WHERE KEY=\"jsmith\"",
                
//                "DELETE \"first\" FROM JdbcBytes WHERE KEY=\"jsmith\"",
//                "SELECT \"first\" FROM JdbcBytes WHERE KEY=\"jsmith\"",
//                "SELECT \"last\" FROM JdbcBytes WHERE KEY=\"jsmith\"",
                
                "DELETE \"first\" FROM JdbcUtf8 WHERE KEY=\"jsmith\"",
                "SELECT \"first\" FROM JdbcUtf8 WHERE KEY=\"jsmith\"",
                "SELECT \"last\" FROM JdbcUtf8 WHERE KEY=\"jsmith\"",
        };
        
        for (int i = 0; i < statements.length/3; i++) 
        {
            executeNoResults(con, statements[3*i]);
            ResultSet rs = executePreparedStatementWithResults(con, statements[3*i+1]);
            assert !rs.next() : statements[3*i+1];
            rs.close();
            rs = executePreparedStatementWithResults(con, statements[3*i+2]);
            assert rs.next() : statements[3*i+2];
        }
    }

    @AfterClass
    public static void stopServer() throws SQLException
    {
        if (con != null)
        {
            String[] stmts = 
            {
//                "TRUNCATE Standard1",
//                "TRUNCATE JcbcAscii", // todo: this one is broken for some reason.
                "TRUNCATE JdbcInteger",
                "TRUNCATE JdbcLong",
//                "TRUNCATE JdbcBytes",
                "TRUNCATE JdbcUtf8",
            };
            for (String stmt : stmts)
            {
                try 
                {
                    executeNoResults(con, stmt);
                }
                catch (SQLException ex)
                {
                    throw new SQLException(stmt, ex);
                }
            }
            con.close();
            con = null;
        }
    }
    
    // todo: check expected values as well.
    /** iterates over a result set checking columns */
    private static void checkResultSet(ResultSet rs, String accessor, int expectedRows, String... cols) throws SQLException
    {
        int actualRows = 0;
        assert rs != null;
        while (rs.next())
        {
            actualRows++;
            for (int c = 0; c < cols.length; c++)
            {
                // getString and getObject should always work.
                assert rs.getString(cols[c]) != null;
                assert rs.getString(c) != null;
                assert rs.getObject(cols[c]) != null;
                assert rs.getObject(c) != null;
                
                // now call the accessor.
                try
                {
                    Method byInt = rs.getClass().getDeclaredMethod("get" + accessor, int.class);
                    byInt.setAccessible(true);
                    assert byInt.invoke(rs, c) != null;
                    
                    Method byString = rs.getClass().getDeclaredMethod("get" + accessor, String.class);
                    byString.setAccessible(true);
                    assert byString.invoke(rs, cols[c]) != null;
                }
                catch (NoSuchMethodException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (IllegalAccessException ex)
                {
                    throw new RuntimeException(ex);
                }
                catch (InvocationTargetException ex) 
                {
                    throw new RuntimeException(ex);
                }
            }
        }
        
        assert actualRows == expectedRows;
    }
    
    /** executes a prepared statement */
    private static ResultSet executePreparedStatementWithResults(final Connection con, final String selectQ) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(selectQ);
        return statement.executeQuery();
    }

    /** executes an prepared statement */
    private static void executeNoResults(final Connection con, final String cql) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(cql);
        statement.execute();
    }
}
