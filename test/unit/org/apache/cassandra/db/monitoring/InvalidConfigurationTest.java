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

package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.MonitoringService;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;

import org.junit.*;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InvalidConfigurationTest extends CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(InvalidConfigurationTest.class);
    private String ksNetwork = null;
    private String ksSimple = null;

    @Test
    public void testCheckForInvalidConsistencyNetworkTopology()
    {
        String selectQuery = String.format("SELECT * FROM %s.tb1;", ksNetwork);
        String insertQuery = String.format("INSERT INTO %s.tb1 (a, b) VALUES (?, ?);", ksNetwork);

        assertEquals(0, DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL));

        // ONE -> LOCAL_ONE
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ONE));

        // The same CL for the same type of read / write for each table is reported only once.
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ONE));

        // LOCAL_ONE for read is allowed.
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.LOCAL_ONE));

        // QUORUM -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.QUORUM));

        // EACH_QUORUM -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.EACH_QUORUM));

        // SERIAL -> LOCAL_SERIAL
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.SERIAL));

        // TWO -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.TWO));

        // THREE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.THREE));

        // ALL -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ALL));

        // ANY -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ANY));

        // Test for write and NetworkTopology strategy
        // ONE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ONE, 1, 1));

        // LOCAL_ONE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.LOCAL_ONE, 2, 1));

        //  LOCAL_QUORUM is right
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.LOCAL_QUORUM, 3, 1));

        // TWO -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.TWO, 4, 1));

        // THREE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.THREE, 5, 1));

        // ALL -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ALL, 6, 1));
    }


    @Test
    public void testCheckForInvalidConsistencySimpleTopology()
    {
        String insertQuery = String.format("INSERT INTO %s.tb2 (a, b) VALUES (?, ?);", ksSimple);
        String selectQuery = String.format("SELECT * FROM %s.tb2;", ksSimple);

        assertEquals(0, DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL));

        // We don't check QUORUM / ONE / SERIAL for SimpleStrategy
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.QUORUM));
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ONE));

        // TWO -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.TWO));

        // ANY -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(selectQuery, ConsistencyLevel.ANY));

        // Test for write and SimpleStrategy
        // ONE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ONE, 1, 1));

        // LOCAL_ONE -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.LOCAL_ONE, 2, 1));

        // We don't check QUORUM / ONE / SERIAL for SimpleStrategy
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.QUORUM, 3, 1));

        // Duplicate for the same table, CL, and write
        assertEquals(0, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ONE, 4, 1));

        // ANY -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ANY, 5, 1));

        // ALL -> LOCAL_QUORUM
        assertEquals(1, getIncorrectConsistencyLevelStatsAfterApplyQuery(insertQuery, ConsistencyLevel.ALL, 6, 1));
    }

    @Before
    public void creatTable() throws Throwable
    {
        requireNetwork();
        ksNetwork = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class' : 'NetworkTopologyStrategy', '"
                                   + DATA_CENTER + "' : 3 } AND durable_writes=true");
        execute(String.format("CREATE TABLE %s.tb1 (a int PRIMARY KEY, b int);", ksNetwork));

        ksSimple = createKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                  "{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 } AND durable_writes=true");
        execute(String.format("CREATE TABLE %s.tb2 (a int PRIMARY KEY, b int);", ksSimple));
        
        BadQuery.setup();
        MonitoringService.instance.setBadQueryTracingFraction(1.0);
    }

    @After
    public void dropCreatedTable()
    {
        try
        {
            QueryProcessor.executeOnceInternal("DROP KEYSPACE " + ksNetwork);
            QueryProcessor.executeOnceInternal("DROP KEYSPACE " + ksSimple);
        }
        catch (Throwable t)
        {
            // ignore
        }
        ((BadQueriesInSystemLog) DatabaseDescriptor.getBadQueryReporter()).clear();
    }

    private int getIncorrectConsistencyLevelStatsAfterApplyQuery(String query, ConsistencyLevel cl, Object... values)
    {
        try
        {
            QueryProcessor.execute(query, cl, values);
        }
        catch (Exception ex)
        {
            // ignore
        }
        int count = DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL);

        ((BadQueriesInSystemLog) DatabaseDescriptor.getBadQueryReporter()).clear();
        assertEquals(0, DatabaseDescriptor.getBadQueryReporter().getStats(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL));
        return count;
    }
}
