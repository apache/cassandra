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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang3.stream.Streams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.tracing.TraceKeyspace;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccordTracingTest extends AccordTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(AccordTracingTest.class);

    @Override
    protected Logger logger()
    {
        return logger;
    }

    @BeforeClass
    public static void setupClass() throws IOException
    {
        AccordTestBase.setupCluster(builder -> builder.withConfig(config -> config.with(Feature.NATIVE_PROTOCOL, Feature.NETWORK)), 3);
    }

    private static Set<Pattern> expectedThreads(String... patterns)
    {
        return Streams.of(patterns).map(Pattern::compile).collect(toSet());
    }

    @Test
    public void testTracing() throws Throwable
    {
        test("CREATE TABLE " + qualifiedAccordTableName + " (k int, c int, v int, PRIMARY KEY (k, c)) WITH transactional_mode='full'", cluster -> {
            UUID sessionId = TimeUUID.minAtUnixMillis(System.currentTimeMillis()).asUUID();
            logger.info("sessionId={}", sessionId);
            cluster.coordinator(1).executeWithTracing(sessionId,"INSERT INTO " + qualifiedAccordTableName + " (k,c,  v) VALUES (?, ?, ?)", ConsistencyLevel.QUORUM, 42, 84, 168);

            // Give trace events time to execute
            Thread.sleep(2000);

            // Keep track of expected trace threads to make sure they don't stop emitting trace messages
            // and if new ones appear error out so they get added to the list
            Set<Pattern> expectedThreads = expectedThreads("node\\d+_ReadStage-\\d+",
                                                           "node\\d+_CommandStore\\[\\d+\\]:\\d+",
                                                           "node\\d+_Messaging-EventLoop-\\d+-\\d+",
                                                           "node\\d+_MutationStage-\\d+",
                                                           "node\\d+_isolatedExecutor:\\d+"
                                                           );
            Set<Pattern> threadsNotYetSeen = expectedThreads;
            Set<String> unexpectedThreads = new HashSet<>();
            // Check that traces appear
            SimpleQueryResult result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.SESSIONS, ConsistencyLevel.QUORUM);
            int sessionCount = 0;
            while (result.hasNext())
            {
                logger.info(result.next().toString());
                sessionCount++;
            }
            result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.EVENTS, ConsistencyLevel.QUORUM);
            int eventCount = 0;
            while (result.hasNext())
            {
                Row r = result.next();
                logger.info(result.next().toString());
                String thread = r.get("thread");
                boolean wasExpected = expectedThreads.stream().anyMatch(p -> p.matcher(thread).matches());
                if (!wasExpected)
                    unexpectedThreads.add(thread);
                threadsNotYetSeen = threadsNotYetSeen.stream().filter(pattern -> !pattern.matcher(thread).matches()).collect(toSet());
                eventCount++;
            }
            logger.info("Sessions {}, Events {}", sessionCount, eventCount);

            // Check that subsequent transactions don't trace using the wrong session
            List<Future<SimpleQueryResult>> queries = new ArrayList<>();
            for (int ii = 1000; ii < 2000; ii++)
                queries.add(cluster.coordinator(1).asyncExecuteWithResult("INSERT INTO " + qualifiedAccordTableName + " (k,c,  v) VALUES (?, ?, ?)", ConsistencyLevel.QUORUM, ii, ii, ii));
            queries.forEach(Futures::getUnchecked);

            result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.SESSIONS, ConsistencyLevel.QUORUM);
            int afterSessionCount = 0;
            while (result.hasNext())
            {
                logger.info(result.next().toString());
                afterSessionCount++;
            }

            result = cluster.coordinator(1).executeWithResult("SELECT * FROM " + SchemaConstants.TRACE_KEYSPACE_NAME + "." + TraceKeyspace.EVENTS, ConsistencyLevel.QUORUM);
            int afterEventCount = 0;
            while (result.hasNext())
            {
                logger.info(result.next().toString());
                afterEventCount++;
            }
            logger.info("Sessions {}, Events {}", afterSessionCount, afterEventCount);
            assertEquals(sessionCount, afterSessionCount);
            assertEquals(eventCount, afterEventCount);

            if (!threadsNotYetSeen.isEmpty())
                logger.info("Didn't find matches for patterns " + threadsNotYetSeen);
            assertTrue("All expected thread traces should be present", threadsNotYetSeen.isEmpty());
            if (!unexpectedThreads.isEmpty())
                logger.info("Found threads new threads: {}", unexpectedThreads);
            assertTrue("Unexpected thread created trace messages, add them to the list of expected threads?", unexpectedThreads.isEmpty());
        });
    }
}
