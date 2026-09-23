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
import java.util.List;
import java.util.function.Consumer;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.exceptions.ReadTimeoutException;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_READ_ITERATION_DELAY_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TimeoutAbortTest extends TestBaseImpl
{
    private static final String[] UNEXPECTED_RT_BOUND_ERRORS = { "open RT bound", "Range Tombstones to be closed" };

    @Test
    public void timeoutTest() throws IOException, InterruptedException
    {
        TEST_READ_ITERATION_DELAY_MS.setInt(5000);
        timeoutTestWithAssertionConsumer(1000, errors -> assertFalse(errors.toString(), errors.stream().anyMatch(s -> s.contains("open RT bound"))));
    }

    @Test
    public void timeoutMidRangeTombstoneTest() throws IOException, InterruptedException
    {
        TEST_READ_ITERATION_DELAY_MS.setInt(1000);
        timeoutTestWithAssertionConsumer(0, errors -> {
            for (String unexpectedError : UNEXPECTED_RT_BOUND_ERRORS)
                assertFalse("a truncated range tombstone was reported as an error: " + errors,
                            errors.stream().anyMatch(s -> s.contains(unexpectedError) && s.contains("ERROR")));
        });
    }

    private static void timeoutTestWithAssertionConsumer(long sleepMillis, Consumer<List<String>> assertionConsumer) throws IOException, InterruptedException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int, ck1 int, ck2 int, d int, primary key (id, ck1, ck2))"));
            cluster.coordinator(1).execute(withKeyspace("delete from %s.tbl using timestamp 5 where id = 1 and ck1 = 77 "), ConsistencyLevel.ALL);
            cluster.get(1).flush(KEYSPACE);
            if (sleepMillis > 0) Thread.sleep(sleepMillis);
            for (int i = 0; i < 100; i++)
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, ck1, ck2, d) values (1,77,?,1) using timestamp 10"), ConsistencyLevel.ALL, i);
            cluster.get(1).flush(KEYSPACE);
            try
            {
                cluster.coordinator(1).execute(withKeyspace("select * from %s.tbl where id=1 and ck1 = 77"), ConsistencyLevel.ALL);
                fail("the read should not have completed; the iteration delay must abort it");
            }
            catch (Exception e)
            {
                assertEquals("expected a ReadTimeoutException, got " + e.getClass().getName() + ": " + e.getMessage(),
                             ReadTimeoutException.class.getName(), e.getClass().getName());
            }
            List<String> errors = cluster.get(1).logs().grepForErrors().getResult();
            assertionConsumer.accept(errors);
        }
    }
}
