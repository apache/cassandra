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
package org.apache.cassandra.index.sai.functional;

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.IndexNotAvailableException;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.inject.Injection;
import org.apache.cassandra.inject.Injections;
import org.assertj.core.api.Assertions;

import static org.junit.Assert.assertEquals;

public class FailureTest extends SAITester
{
    @Test
    public void shouldMakeIndexNonQueryableOnSSTableContextFailureDuringFlush() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '1')");
        execute("INSERT INTO %s (id1, v1) VALUES ('2', '2')");
        flush();

        assertEquals(1, execute("SELECT id1 FROM %s WHERE v1='1'").size());

        verifyIndexFiles(literalIndexContext, 1, 1, 1);
        verifySSTableIndexes(literalIndexContext.getIndexName(), 1, 1);

        execute("INSERT INTO %s (id1, v1) VALUES ('3', '3')");

        Injection ssTableContextCreationFailure = newFailureOnEntry("context_failure_on_flush", SSTableContext.class, "<init>", RuntimeException.class);
        Injections.inject(ssTableContextCreationFailure);

        flush();

        // Verify that, while the node is still operational, the index is not.
        Assertions.assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE v1='1'"))
                  .isInstanceOf(IndexNotAvailableException.class);

        ssTableContextCreationFailure.disable();

        // Now verify that a restart actually repairs the index...
        simulateNodeRestart();

        verifyIndexFiles(literalIndexContext, 2);
        verifySSTableIndexes(literalIndexContext.getIndexName(), 2, 2);

        assertEquals(1, execute("SELECT id1 FROM %s WHERE v1='1'").size());
    }

    @Test
    public void shouldMakeIndexNonQueryableOnSSTableContextFailureDuringCompaction() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        IndexContext literalIndexContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '1')");
        flush();

        execute("INSERT INTO %s (id1, v1) VALUES ('2', '2')");
        flush();

        assertEquals(1, execute("SELECT id1 FROM %s WHERE v1='1'").size());

        verifyIndexFiles(literalIndexContext, 2, 2, 2);
        verifySSTableIndexes(literalIndexContext.getIndexName(), 2, 2);

        Injection ssTableContextCreationFailure = newFailureOnEntry("context_failure_on_compaction", SSTableContext.class, "<init>", RuntimeException.class);
        Injections.inject(ssTableContextCreationFailure);

        compact();

        // Verify that the index is not available.
        Assertions.assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE v1='1'"))
                  .isInstanceOf(IndexNotAvailableException.class);
    }

    @Test
    public void shouldMakeIndexNonQueryableOnSSTableContextFailureDuringCreation() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);

        execute("INSERT INTO %s (id1, v1) VALUES ('1', '1')");
        execute("INSERT INTO %s (id1, v1) VALUES ('2', '2')");

        Injection ssTableContextCreationFailure = newFailureOnEntry("context_failure_on_creation", SSTableContext.class, "<init>", RuntimeException.class);
        Injections.inject(ssTableContextCreationFailure);

        String v1IndexName = createIndexAsync(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        // Verify that the initial index build fails...
        verifyInitialIndexFailed(v1IndexName);

        verifyNoIndexFiles();
        verifySSTableIndexes(v1IndexName, 0);

        // ...and then verify that, while the node is still operational, the index is not.
        Assertions.assertThatThrownBy(() -> execute("SELECT * FROM %s WHERE v1='1'"))
                  .isInstanceOf(IndexNotAvailableException.class);
    }
}
