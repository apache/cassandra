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

import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CUSTOM_DISK_ERROR_HANDLER;
import static org.apache.cassandra.service.DiskErrorsHandlerService.get;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DiskErrorsHandlerTest
{
    @Test
    public void testSetting() throws Throwable
    {
        DiskErrorsHandler handlerA;
        DiskErrorsHandler handlerB;
        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              HandlerA.class.getName()))
        {
            DiskErrorsHandlerService.configure();
            handlerA = get();
            assertSame(HandlerA.class, handlerA.getClass());
            assertInitialized(HandlerA.class, handlerA);
            assertNotClosed(HandlerA.class, handlerA);
        }

        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              HandlerB.class.getName()))
        {
            DiskErrorsHandlerService.configure();

            handlerB = get();
            assertSame(HandlerB.class, handlerB.getClass());

            assertInitialized(HandlerA.class, handlerA);
            assertClosed(HandlerA.class, handlerA);

            assertInitialized(HandlerB.class, handlerB);
            assertNotClosed(HandlerB.class, handlerB);

            handlerB.close();

            assertClosed(HandlerB.class, handlerB);
        }
    }

    @Test
    public void testFailures()
    {
        DiskErrorsHandler handlerC;
        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              HandlerC.class.getName()))
        {
            DiskErrorsHandlerService.configure();
            handlerC = get();
            assertInitialized(HandlerC.class, handlerC);
        }

        DiskErrorsHandler handlerA;
        // this will call _not_ close() on C handler
        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              HandlerA.class.getName()))
        {
            DiskErrorsHandlerService.configure();
            handlerA = get();
            assertInitialized(HandlerA.class, handlerA);
            assertNotClosed(HandlerC.class, handlerC);
        }

        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              HandlerD.class.getName()))
        {
            assertThatThrownBy(DiskErrorsHandlerService::configure)
            .isInstanceOf(ConfigurationException.class);

            assertSame(HandlerA.class, get().getClass());
            // still handler A as handler D failed to init
            assertInitialized(HandlerA.class, handlerA);
            assertNotClosed(HandlerA.class, handlerA);
        }

        // what if a user tries to set no-op handler or handler which can not be constructed (constructor is private)
        try (WithProperties ignore = new WithProperties().set(CUSTOM_DISK_ERROR_HANDLER,
                                                              DiskErrorsHandler.NoOpDiskErrorHandler.class.getName()))
        {
            assertThatThrownBy(DiskErrorsHandlerService::configure)
            .isInstanceOf(ConfigurationException.class)
            .hasMessageContaining("Default constructor for disk error handler class " +
                                  '\'' + DiskErrorsHandler.NoOpDiskErrorHandler.class.getName() + "' is inaccessible.");
        }
    }

    public static class HandlerA extends DummyErrorHandler {}

    public static class HandlerB extends DummyErrorHandler {}

    public static class HandlerC extends DummyErrorHandler
    {
        @Override
        public void close() throws Exception
        {
            throw new RuntimeException("failed to close");
        }
    }

    public static class HandlerD extends DummyErrorHandler
    {
        @Override
        public void init()
        {
            throw new RuntimeException("failed to init");
        }
    }

    public void assertClosed(Class<?> handlerClass, DiskErrorsHandler diskErrorsHandler)
    {
        assertSame(handlerClass, diskErrorsHandler.getClass());
        assertTrue(((DummyErrorHandler) diskErrorsHandler).closed);
    }

    public void assertNotClosed(Class<?> handlerClass, DiskErrorsHandler diskErrorsHandler)
    {
        assertSame(handlerClass, diskErrorsHandler.getClass());
        assertFalse(((DummyErrorHandler) diskErrorsHandler).closed);
    }

    public void assertInitialized(Class<?> handlerClass, DiskErrorsHandler diskErrorsHandler)
    {
        assertSame(handlerClass, diskErrorsHandler.getClass());
        assertTrue(((DummyErrorHandler) diskErrorsHandler).initialized);
    }

    private static abstract class DummyErrorHandler implements DiskErrorsHandler
    {
        public boolean initialized = false;
        public boolean closed = false;

        @Override
        public void init()
        {
            initialized = true;
        }

        @Override
        public void close() throws Exception
        {
            closed = true;
        }

        @Override
        public void handleCorruptSSTable(CorruptSSTableException e)
        {
        }

        @Override
        public void handleFSError(FSError e)
        {
        }

        @Override
        public void handleStartupFSError(Throwable t)
        {
        }

        @Override
        public void inspectDiskError(Throwable t)
        {
        }

        @Override
        public void inspectCommitLogError(Throwable t)
        {
        }

        @Override
        public boolean handleCommitError(String message, Throwable t)
        {
            return true;
        }
    }
}
