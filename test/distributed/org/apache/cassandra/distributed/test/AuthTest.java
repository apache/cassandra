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
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertTrue;

public class AuthTest extends TestBaseImpl
{

    /**
     * Simply tests that initialisation of a test Instance results in
     * StorageService.instance.doAuthSetup being called as the regular
     * startup does in CassandraDaemon.setup
     */
    @Test
    public void authSetupIsCalledAfterStartup() throws IOException
    {
        try (Cluster cluster = Cluster.build().withNodes(1).start())
        {
            boolean setupCalled = cluster.get(1).callOnInstance(() -> {
                long maxWait = TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);
                long start = System.nanoTime();
                while (!StorageService.instance.authSetupCalled() && System.nanoTime() - start < maxWait)
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                return StorageService.instance.authSetupCalled();
            });
            assertTrue(setupCalled);
        }
    }
}
