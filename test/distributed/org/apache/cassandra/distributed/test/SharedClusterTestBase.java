/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.Cluster.Builder;

/**
 * Base class for distributed tests that use a shared cluster across multiple test methods.
 * This provides the same shared cluster lifecycle management as AccordTestBase but without
 * the Accord-specific functionality.
 */
public abstract class SharedClusterTestBase extends TestBaseImpl
{
    protected static final AtomicInteger COUNTER = new AtomicInteger(0);
    protected static Cluster SHARED_CLUSTER;

    protected String tableName;
    protected String qualifiedTableName;

    /**
     * Sets up the shared cluster. Subclasses should call this from their @BeforeClass method.
     *
     * @param nodes the number of nodes in the cluster
     * @param options function to customize the cluster builder
     * @throws IOException if cluster creation fails
     */
    protected static void setupCluster(int nodes, Function<Builder, Builder> options) throws IOException
    {
        SHARED_CLUSTER = options.apply(Cluster.build().withNodes(nodes)).start();
    }

    /**
     * Sets up keyspace and tables for the test suite. Called once per test class.
     * Subclasses should override this to customize keyspace and table creation.
     */
    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // Subclasses should override to create keyspace and tables
    }

    /**
     * Tears down keyspace and cluster. Called once per test class.
     */
    @AfterClass
    public static void tearDownClass()
    {
        if (SHARED_CLUSTER != null)
        {
            try
            {
                // Drop the test keyspace
                SHARED_CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            }
            catch (Exception e)
            {
                // Ignore errors during cleanup
            }
            SHARED_CLUSTER.close();
            SHARED_CLUSTER = null;
        }
    }

    /**
     * Sets up test instance variables before each test method.
     */
    @Before
    public void setUp()
    {
        tableName = "tbl" + COUNTER.getAndIncrement();
        qualifiedTableName = KEYSPACE + '.' + tableName;
    }

    /**
     * Cleanup after each test method. Truncates tables created by the test.
     */
    @After
    public void tearDown()
    {
        if (SHARED_CLUSTER != null)
        {
            try
            {
                // Truncate tables to clean up data between tests
                truncateTables();
            }
            catch (Exception e)
            {
                // Ignore errors during cleanup
            }
        }
    }

    /**
     * Truncates tables after each test. Subclasses should override to specify which tables to truncate.
     */
    protected void truncateTables()
    {
        // Default implementation does nothing, subclasses should override
    }

    /**
     * Creates the keyspace with the specified replication strategy. 
     * Subclasses should call this from their setUpClass method.
     */
    protected static void createKeyspace(String replicationStrategy)
    {
        SHARED_CLUSTER.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = " + replicationStrategy);
    }

    /**
     * Creates a table with the specified DDL.
     * Subclasses should call this from their setUpClass method.
     */
    protected static void createTable(String tableDDL)
    {
        SHARED_CLUSTER.schemaChange(tableDDL);
    }
}