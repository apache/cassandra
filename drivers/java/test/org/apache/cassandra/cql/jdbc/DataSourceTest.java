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
package org.apache.cassandra.cql.jdbc;

import static org.junit.Assert.*;

import java.io.PrintWriter;
import java.sql.SQLFeatureNotSupportedException;

import javax.sql.DataSource;

import org.apache.cassandra.cql.EmbeddedServiceBase;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataSourceTest extends EmbeddedServiceBase
{
    private static final String HOST = "localhost";
    private static final int PORT = 9170;
    private static final String KEYSPACE = "Test";
    private static final String USER = "JohnDoe";
    private static final String PASSWORD = "secret";


    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        startCassandraServer();
    }

    @Test
    public void testConstructor() throws Exception
    {
        CassandraDataSource cds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD);
        assertEquals(HOST,cds.getServerName());
        assertEquals(PORT,cds.getPortNumber());
        assertEquals(KEYSPACE,cds.getDatabaseName());
        assertEquals(USER,cds.getUser());
        assertEquals(PASSWORD,cds.getPassword());
        
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD);
        assertNotNull(ds);
        
        PrintWriter pw = new PrintWriter(System.err);
        
        // null username and password
        java.sql.Connection cnx = ds.getConnection(null, null);
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());
        ds.setLogWriter(pw);
        assertNotNull(ds.getLogWriter());
        
        // no username and password
        cnx = ds.getConnection();
        assertFalse(cnx.isClosed());
        ds.setLoginTimeout(5);
        assertEquals(5, ds.getLoginTimeout());
        ds.setLogWriter(pw);
        assertNotNull(ds.getLogWriter());
    }

    
    @Test
    public void testIsWrapperFor() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD);
        
        boolean isIt = false;
                
        // it is a wrapper for DataSource
        isIt = ds.isWrapperFor(DataSource.class);        
        assertTrue(isIt);
        
        // it is not a wrapper for this test class
        isIt = ds.isWrapperFor(this.getClass());        
        assertFalse(isIt);
    }
 
    @Test(expected=SQLFeatureNotSupportedException.class)
    public void testUnwrap() throws Exception
    {
        DataSource ds = new CassandraDataSource(HOST,PORT,KEYSPACE,USER,PASSWORD);

        // it is a wrapper for DataSource
        DataSource newds = ds.unwrap(DataSource.class);        
        assertNotNull(newds);
        
        // it is not a wrapper for this test class
        newds = (DataSource) ds.unwrap(this.getClass());        
        assertNotNull(newds);
    }
}
