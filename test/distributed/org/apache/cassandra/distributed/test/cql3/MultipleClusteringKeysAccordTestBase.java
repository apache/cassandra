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

package org.apache.cassandra.distributed.test.cql3;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.accord.AccordTestBase;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

/**
 * Tests for CASSANDRA-20828: Multiple clustering keys bug in transactions.
 * 
 * The original bug occurred when single CQL statements (UPDATE/DELETE) used IN clauses 
 * on clustering columns within transactions, causing createClustering() to return multiple 
 * Clustering objects but getTxnReferenceOps() called Iterables.getOnlyElement() expecting only one.
 * 
 * This class tests the specific scenarios that trigger the original bug:
 * - Single UPDATE statement with IN clause on clustering columns  
 * - Single DELETE statement with IN clause on clustering columns
 * 
 * Note: Tests with multiple separate statements in transactions do NOT trigger this bug
 * as each statement generates a single clustering key.
 */
public abstract class MultipleClusteringKeysAccordTestBase extends AccordTestBase
{
    protected final String transactionalModeString;

    public MultipleClusteringKeysAccordTestBase(TransactionalMode mode)
    {
        super(mode);
        this.transactionalModeString = mode.name();
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder, 1);
    }

    @Test
    public void testUPDATEWithINClauseOnClusteringColumn()
    {
        // Single clustering key table to test IN clause properly
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH transactional_mode='" + transactionalModeString + "'");
        
        // Insert test data
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 10, 100)", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 20, 200)", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 30, 300)", ConsistencyLevel.QUORUM);
        
        // Single UPDATE with IN clause - triggers multiple clustering keys in createClustering()
        // Before fix: would fail with "expected one element but was" from Iterables.getOnlyElement()
        // After fix: properly handles List<Clustering<?>> with multiple elements
        SHARED_CLUSTER.coordinator(1).execute("BEGIN TRANSACTION UPDATE " + qualifiedAccordTableName + " SET v = 999 WHERE pk = 1 AND ck IN (10, 20); COMMIT TRANSACTION", ConsistencyLevel.QUORUM);
        
        // Verify the updates
        assertRows(SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE pk = 1", ConsistencyLevel.QUORUM),
                  row(1, 10, 999),
                  row(1, 20, 999),
                  row(1, 30, 300));
    }

    @Test
    public void testDELETEWithINClauseOnClusteringColumn()
    {
        // Single clustering key table to test IN clause properly
        SHARED_CLUSTER.schemaChange("CREATE TABLE " + qualifiedAccordTableName + " (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH transactional_mode='" + transactionalModeString + "'");
        
        // Insert test data
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 10, 100)", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 20, 200)", ConsistencyLevel.QUORUM);
        SHARED_CLUSTER.coordinator(1).execute("INSERT INTO " + qualifiedAccordTableName + " (pk, ck, v) VALUES (1, 30, 300)", ConsistencyLevel.QUORUM);
        
        // Single DELETE with IN clause - triggers multiple clustering keys in createClustering()
        // Before fix: would fail with "expected one element but was" from Iterables.getOnlyElement()
        // After fix: properly handles List<Clustering<?>> with multiple elements
        SHARED_CLUSTER.coordinator(1).execute("BEGIN TRANSACTION DELETE FROM " + qualifiedAccordTableName + " WHERE pk = 1 AND ck IN (10, 20); COMMIT TRANSACTION", ConsistencyLevel.QUORUM);
        
        // Verify the deletes
        assertRows(SHARED_CLUSTER.coordinator(1).execute("SELECT * FROM " + qualifiedAccordTableName + " WHERE pk = 1", ConsistencyLevel.QUORUM),
                  row(1, 30, 300));
    }
}