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

package org.apache.cassandra.hints;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.awaitility.Awaitility;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.hints.HintsTestUtil.MockFailureDetector;
import static org.apache.cassandra.hints.HintsTestUtil.sendHintsAndResponses;
import static org.junit.Assert.assertEquals;

@RunWith(BMUnitRunner.class)
public class HintServiceBytemanTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";

    private final MockFailureDetector failureDetector = new HintsTestUtil.MockFailureDetector();
    private static TableMetadata metadata;

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE));
        metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Before
    public void reinstanciateService() throws Throwable
    {
        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        failureDetector.isAlive = true;

        HintsService.instance = new HintsService(failureDetector);

        HintsService.instance.startDispatch();
    }

    @Test
    @BMRule(name = "Delay delivering hints",
    targetClass = "DispatchHintsTask",
    targetMethod = "run",
    action = "Thread.sleep(DatabaseDescriptor.getHintsFlushPeriodInMS() * 3L)")
    public void testListPendingHints() throws InterruptedException, ExecutionException
    {
        HintsService.instance.resumeDispatch();
        MockMessagingSpy spy = sendHintsAndResponses(metadata, 20000, -1);
        Awaitility.await("For the hints file to flush")
                  .atMost(Duration.ofMillis(DatabaseDescriptor.getHintsFlushPeriodInMS() * 2L))
                  .until(() -> !HintsService.instance.getPendingHints().isEmpty());

        List<PendingHintsInfo> pendingHints = HintsService.instance.getPendingHintsInfo();
        assertEquals(1, pendingHints.size());
        PendingHintsInfo info = pendingHints.get(0);
        assertEquals(StorageService.instance.getLocalHostUUID(), info.hostId);
        assertEquals(1, info.totalFiles);
        assertEquals(info.oldestTimestamp, info.newestTimestamp); // there is 1 descriptor with only 1 timestamp

        spy.interceptMessageOut(20000).get();
        assertEquals(Collections.emptyList(), HintsService.instance.getPendingHints());
    }
}
