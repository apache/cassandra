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

package org.apache.cassandra.service;

import java.util.function.BooleanSupplier;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import io.netty.channel.EventLoopGroup;

import static org.apache.cassandra.service.NativeTransportServiceTest.withService;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NativeTransportManagementServiceTest
{
    @BeforeClass
    public static void setupTransport()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setStartNativeTransportManagement(true);
    }

    @AfterClass
    public static void cleanupManagementConfig()
    {
        DatabaseDescriptor.setStartNativeTransportManagement(false);
    }

    @Test
    public void testStart()
    {
        withService((CassandraDaemon.Server service) -> assertTrue(service.isRunning()),
                    NativeTransportManagementService::new, true, 1);
    }

    @Test
    public void testDestroy()
    {
        withService((CassandraDaemon.Server srv) -> {
            NativeTransportManagementService service = (NativeTransportManagementService) srv;
            EventLoopGroup workerGroup = service.getWorkerGroup();
            BooleanSupplier allTerminated = () -> workerGroup != null
                                                  && workerGroup.isShutdown()
                                                  && workerGroup.isTerminated();

            assertFalse(allTerminated.getAsBoolean());
            service.destroy();
            assertTrue(allTerminated.getAsBoolean());
        }, NativeTransportManagementService::new, true, 1);
    }

    @Test
    public void testConcurrentDestroys()
    {
        withService(srv -> ((NativeTransportManagementService) srv).destroy(),
                    NativeTransportManagementService::new, true, 20);
    }
}
