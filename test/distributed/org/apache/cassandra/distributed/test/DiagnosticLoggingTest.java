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
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.diag.DiagnosticLogOptions;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.io.util.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DiagnosticLoggingTest extends TestBaseImpl
{
    private static Cluster cluster;
    private static IInvokableInstance node;
    private static final DiagnosticLogOptions options = new DiagnosticLogOptions();

    @BeforeClass
    public static void beforeTest() throws IOException
    {
        options.diagnostic_log_dir = Files.createTempDirectory("diagnosticlogdir").toString();
        cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.GOSSIP)
                                                         .with(Feature.NETWORK)
                                                         .with(Feature.NATIVE_PROTOCOL)
                                                         .set("diagnostic_events_enabled", true)
                                                         .set("diagnostic_logging_options", options)).start());
        node = cluster.get(1);
    }

    @AfterClass
    public static void afterTest() throws Exception
    {
        FileUtils.deleteDirectory(new File(options.diagnostic_log_dir).toJavaIOFile());
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testDiagnosticLoggingEnableAndDisable()
    {
        boolean diagnosticEnabled = node.callOnInstance((SerializableCallable<Boolean>) () -> DiagnosticEventService.instance().isDiagnosticsEnabled());
        boolean diagnosticLogEnabled = node.callOnInstance((SerializableCallable<Boolean>) () -> DiagnosticEventService.instance().isDiagnosticLogEnabled());

        assertTrue(diagnosticEnabled);
        assertFalse(diagnosticLogEnabled);

        node.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> DiagnosticEventService.instance().enableDiagnosticLog());
        assertTrue(node.callOnInstance((SerializableCallable<Boolean>) () -> DiagnosticEventService.instance().isDiagnosticLogEnabled()));
        node.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> DiagnosticEventService.instance().disableDiagnosticLog());
        assertFalse(node.callOnInstance((SerializableCallable<Boolean>) () -> DiagnosticEventService.instance().isDiagnosticLogEnabled()));
    }
}
