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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.cassandra.config.ConfigurationException;
import org.junit.Test;

/**
 * Test case for unit test of various methods of JDBC implementation.
 */
public class JdbcDriverTest extends EmbeddedServiceBase
{
    private java.sql.Connection con = null;

    /** SetUp */
    @Override
    protected void setUp()
    {
        try
        {
            startCassandraServer();
            Class.forName("org.apache.cassandra.cql.driver.jdbc.CassandraDriver");
            con = DriverManager.getConnection("jdbc:cassandra:root/root@localhost:9170/Keyspace1");
            final String updateQ = "UPDATE Standard1 SET \"first\" = \"firstrec\", \"last\" = \"lastrec\" WHERE KEY = \"jsmith\"";
            executeNoResults(con, updateQ);
        }
        catch (ClassNotFoundException e)
        {
            fail(e.getMessage());
        }
        catch(IOException ioex)
        {
            fail(ioex.getMessage());
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        } 
        catch (ConfigurationException e)
        {
            fail(e.getMessage());
        }
    }

    /** Method to test statement. */
    @Test
    public void testWithStatement()
    {
        try
        {
            String selectQ = "SELECT \"first\", \"last\" FROM Standard1 WHERE KEY=\"jsmith\"";
            Statement stmt = con.createStatement();
            scrollResultset(stmt.executeQuery(selectQ), "first", "last");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

   /** Method to test with prepared statement.*/
   @Test
    public void testWithPreparedStatement()
    {
        try
        {
            final String selectQ = "SELECT \"first\", \"last\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(executePreparedStatementWithResults(con, selectQ), "first", "last");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
        tearDown();
    }

    /** Method to test with update statement.*/
    @Test
    public void testWithUpdateStatement()
    {
        try
        {
            final String updateQ = "UPDATE Standard1 SET \"firstN\" = \"jdbc\", \"lastN\" = \"m\" WHERE KEY = \"jsmith\"";
            executeNoResults(con, updateQ);
            final String updateSelect = "SELECT \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(executePreparedStatementWithResults(con, updateSelect), "firstN", "lastN");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    /* Method to test with Delete statement. */
    @Test
    public void testWithDeleteStatement()
    {
        try
        {
            // Delete
            final String deleteQ = "DELETE \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            executeNoResults(con, deleteQ);
            String updateSelect = "SELECT \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(executePreparedStatementWithResults(con, updateSelect), "firstN", "lastN");
        } 
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    @Override
    protected void tearDown()
    {
        try
        {
            if (con != null)
            {
                final String updateQ = "TRUNCATE Standard1";
                executeNoResults(con, updateQ);
                con.close();
                con = null;
            }
        } 
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    /** iterates over a result set checking columns */
    private static void scrollResultset(final ResultSet rSet, final String... columnNames) throws SQLException
    {
        assertNotNull(rSet);
        while (rSet.next())
        {
            assertNotNull(rSet.getString(0));
            for (String colName : columnNames)
                assertNotNull(rSet.getString(colName));
        }
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
