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

/**
 * Test case for unit test of various methods of JDBC implementation.
 */
public class JdbcDriverTest extends EmbeddedServiceBase
{
    private java.sql.Connection con = null;

    /**
     * SetUp
     */
    @Override
    protected void setUp()
    {
        try
        {
            startCassandraServer();
            Class.forName("org.apache.cassandra.cql.driver.jdbc.CassandraDriver");
            con = DriverManager.getConnection("jdbc:cassandra:root/root@localhost:9170/Keyspace1");
            prepareData();
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

    /**
     * Method to test statement.
     */
    public void testWithStatement()
    {
        try
        {
            scrollResultset(withStatement(con), "first", "last");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

   /**
     * Method to test with prepared statement.
     */
    public void testWithPreparedStatement()
    {
        try
        {
            final String selectQ = "SELECT \"first\", \"last\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(withPreparedStatement(con, selectQ), "first", "last");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
        tearDown();
    }

    /**
     * Method to test with update statement.
     */
    public void testWithUpdateStatement()
    {
        try
        {
            final String updateQ = "UPDATE Standard1 SET \"firstN\" = \"jdbc\", \"lastN\" = \"m\" WHERE KEY = \"jsmith\"";
            withUpdateStatement(con, updateQ);
            final String updateSelect = "SELECT \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(withPreparedStatement(con, updateSelect), "firstN", "lastN");
        }
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    /**
     * Method to test with Delete statement.
     */
    public void testWithDeleteStatement()
    {
        try
        {
            // Delete
            final String deleteQ = "DELETE \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            withDeleteStatement(con, deleteQ);
            String updateSelect = "SELECT \"firstN\", \"lastN\" FROM Standard1 WHERE KEY=\"jsmith\"";
            scrollResultset(withPreparedStatement(con, updateSelect), "firstN", "lastN");
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
                deleteData();
                con.close();
                con = null;
            }
        } 
        catch (SQLException e)
        {
            fail(e.getMessage());
        }
    }

    /**
     * Method to prepare data.
     * 
     * @throws SQLException
     *             sql exception.
     */
    private void prepareData() throws SQLException
    {
        final String updateQ = "UPDATE Standard1 SET \"first\" = \"firstrec\", \"last\" = \"lastrec\" WHERE KEY = \"jsmith\"";
        withUpdateStatement(con, updateQ);
    }

    /**
     * Method to delete data.
     * 
     * @throws SQLException
     *             sql exception.
     */
    private void deleteData() throws SQLException
    {
        final String updateQ = "TRUNCATE Standard1";
        withUpdateStatement(con, updateQ);
    }

    /**
     * With statement method.
     * 
     * @param con
     *            connection object.
     * @return rSet result set.
     * @throws SQLException
     *             sql exception.
     */
    private static ResultSet withStatement(final Connection con) throws SQLException
    {
        String selectQ = "SELECT \"first\", \"last\" FROM Standard1 WHERE KEY=\"jsmith\"";
        Statement stmt = con.createStatement();
        return stmt.executeQuery(selectQ);
    }

    /**
     * With prepared statement method.
     * 
     * @param con
     *            connection object.
     * @return rSet result set.
     * @throws SQLException
     *             sql exception.
     */
    private static ResultSet withPreparedStatement(final Connection con, final String selectQ) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(selectQ);
        return statement.executeQuery();
    }

    /**
     * With scroll result set.
     * 
     * @param rSet
     *            result set.
     * @param columnName
     *            column names.
     * @throws SQLException
     *             sql exception.
     */
    private static void scrollResultset(final ResultSet rSet, final String... columnNames) throws SQLException
    {
        assertNotNull(rSet);
        while (rSet.next())
        {
            assertNotNull(rSet.getString(0));
            assertNotNull(rSet.getString(columnNames[0]));
            assertNotNull(rSet.getString(columnNames[1]));
        }
    }

    /**
     * With update statement
     * 
     * @param con
     *            connection object.
     * @param updateQ
     *            update query.
     * @throws SQLException
     *             sql exception.
     */
    private static void withUpdateStatement(final Connection con, final String updateQ) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(updateQ);
        statement.execute();
    }

    /**
     * With Delete statement
     * 
     * @param con
     *            connection object.
     * @param deleteQ
     *            delete query.
     * @throws SQLException
     *             sql exception.
     */
    private static void withDeleteStatement(final Connection con, final String deleteQ) throws SQLException
    {
        PreparedStatement statement = con.prepareStatement(deleteQ);
        statement.execute();
    }
    /*
     * private static void withCreateStatement(Connection con) throws
     * SQLException { String createQ =
     * "CREATE TABLE JdbcU(\"firstN\", \"lastN\") "; PreparedStatement statement
     * = con.prepareStatement(createQ); statement.execute(); }
     */
}
