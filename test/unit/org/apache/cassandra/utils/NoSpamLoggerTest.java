/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;

import static org.junit.Assert.*;

public class NoSpamLoggerTest
{
    Map<Level, Queue<Pair<String, Object[]>>> logged = new HashMap<>();

    Logger mock = new SubstituteLogger(null, null, true)
    {
        @Override
        public void info(String statement, Object... args)
        {
            logged.get(Level.INFO).offer(Pair.create(statement, args));
        }

        @Override
        public void warn(String statement, Object... args)
        {
            logged.get(Level.WARN).offer(Pair.create(statement, args));
        }

        @Override
        public void error(String statement, Object... args)
        {
            logged.get(Level.ERROR).offer(Pair.create(statement, args));
        }

        @Override
        public int hashCode()
        {
            return 42; //It's a valid hash code
        }

        @Override
        public boolean equals(Object o)
        {
            return this == o;
        }
    };

    static final String statement = "swizzle{}";
    static final String param = "";
    static long now;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        NoSpamLogger.unsafeSetClock(() -> now);
    }

    @Before
    public void setUp() throws Exception
    {
        logged.put(Level.INFO, new ArrayDeque<Pair<String, Object[]>>());
        logged.put(Level.WARN, new ArrayDeque<Pair<String, Object[]>>());
        logged.put(Level.ERROR, new ArrayDeque<Pair<String, Object[]>>());
        NoSpamLogger.clearWrappedLoggersForTest();
    }

    @Test
    public void testNoSpamLogger() throws Exception
    {
        testLevel(Level.INFO);
        testLevel(Level.WARN);
        testLevel(Level.ERROR);
    }

    private void testLevel(Level l) throws Exception
    {
        setUp();
        now = 5;

        assertTrue(NoSpamLogger.log(mock, l, 5, TimeUnit.NANOSECONDS, statement, param));

        assertEquals(1, logged.get(l).size());

        assertFalse(NoSpamLogger.log(mock, l, 5, TimeUnit.NANOSECONDS, statement, param));

        assertEquals(1, logged.get(l).size());

        now += 5;

        assertTrue(NoSpamLogger.log(mock, l, 5, TimeUnit.NANOSECONDS, statement, param));

        assertEquals(2, logged.get(l).size());

        assertTrue(NoSpamLogger.log(mock, l, "key", 5, TimeUnit.NANOSECONDS, statement, param));

        assertEquals(3, logged.get(l).size());

        assertFalse(NoSpamLogger.log(mock, l, "key", 5, TimeUnit.NANOSECONDS, statement, param));

        assertEquals(3, logged.get(l).size());
    }

    private void assertLoggedSizes(int info, int warn, int error)
    {
        assertEquals(info, logged.get(Level.INFO).size());
        assertEquals(warn, logged.get(Level.WARN).size());
        assertEquals(error, logged.get(Level.ERROR).size());
    }

    @Test
    public void testNoSpamLoggerDirect() throws Exception
    {
        now = 5;
        NoSpamLogger logger = NoSpamLogger.getLogger(mock, 5, TimeUnit.NANOSECONDS);

        assertTrue(logger.info(statement, param));
        assertFalse(logger.info(statement, param));
        assertFalse(logger.warn(statement, param));
        assertFalse(logger.error(statement, param));

        assertLoggedSizes(1, 0, 0);

        NoSpamLogStatement statement = logger.getStatement("swizzle2{}", 10, TimeUnit.NANOSECONDS);
        assertTrue(statement.warn(param)); // since a statement of this key hasn't logged yet
        assertLoggedSizes(1, 1, 0);

        now = 10;
        assertFalse(statement.warn(param)); // we logged it above
        assertLoggedSizes(1, 1, 0);

        now = 15;
        assertTrue(statement.warn(param)); // First log was at 5, now past the interval
        assertLoggedSizes(1, 2, 0);
    }

    @Test
    public void testNegativeNowNanos() throws Exception
    {
        now = -6;
        NoSpamLogger logger = NoSpamLogger.getLogger(mock, 5, TimeUnit.NANOSECONDS);

        assertTrue(logger.info(statement, param));
        assertFalse(logger.info(statement, param));
        assertFalse(logger.warn(statement, param));
        assertFalse(logger.error(statement, param));

        assertLoggedSizes(1, 0, 0);

        now = -2;
        assertFalse(logger.error(statement, param));
        assertLoggedSizes(1, 0, 0);

        now = -1;
        assertTrue(logger.error(statement, param));
        assertLoggedSizes(1, 0, 1);

        now = 0;
        assertFalse(logger.error(statement, param));
        assertLoggedSizes(1, 0, 1);

        now = 3;
        assertFalse(logger.error(statement, param));
        assertLoggedSizes(1, 0, 1);

        now = 4;
        assertTrue(logger.info(statement, param));
        assertLoggedSizes(2, 0, 1);
    }

    @Test
    public void testNoSpamLoggerStatementDirect()
    {
        NoSpamLogger.NoSpamLogStatement nospam = NoSpamLogger.getStatement(mock, statement, 5, TimeUnit.NANOSECONDS);

        now = 5;

        assertTrue(nospam.info(statement, param));
        assertFalse(nospam.info(statement, param));
        assertFalse(nospam.warn(statement, param));
        assertFalse(nospam.error(statement, param));

        assertLoggedSizes(1, 0, 0);
    }

    private void checkMock(Level l)
    {
        Pair<String, Object[]> p = logged.get(l).poll();
        assertNotNull(p);
        assertEquals(statement, p.left);
        Object[] objs = p.right;
        assertEquals(1, objs.length);
        assertEquals(param, objs[0]);
        assertTrue(logged.get(l).isEmpty());
    }

    /*
     * Make sure that what is passed to the underlying logger is the correct set of objects
     */
    @Test
    public void testLoggedResult()
    {
        now = 5;

        assertTrue(NoSpamLogger.log(mock, Level.INFO, 5, TimeUnit.NANOSECONDS, statement, param));
        checkMock(Level.INFO);

        now = 10;

        assertTrue(NoSpamLogger.log(mock, Level.WARN, 5, TimeUnit.NANOSECONDS, statement, param));
        checkMock(Level.WARN);

        now = 15;

        assertTrue(NoSpamLogger.log(mock, Level.ERROR, 5, TimeUnit.NANOSECONDS, statement, param));
        checkMock(Level.ERROR);

        now = 20;

        NoSpamLogger logger = NoSpamLogger.getLogger(mock, 5, TimeUnit.NANOSECONDS);

        assertTrue(logger.info(statement, param));
        checkMock(Level.INFO);

        now = 25;

        assertTrue(logger.warn(statement, param));
        checkMock(Level.WARN);

        now = 30;

        assertTrue(logger.error(statement, param));
        checkMock(Level.ERROR);

        NoSpamLogger.NoSpamLogStatement nospamStatement = logger.getStatement(statement);

        now = 35;

        assertTrue(nospamStatement.info(param));
        checkMock(Level.INFO);

        now = 40;

        assertTrue(nospamStatement.warn(param));
        checkMock(Level.WARN);

        now = 45;

        assertTrue(nospamStatement.error(param));
        checkMock(Level.ERROR);
    }

    @Test
    public void testSupplierLogging()
    {
        AtomicInteger evaluationTimes = new AtomicInteger();
        Object [] params = new Object[] {"hello"};
        Supplier<Object[]> paramSupplier = () -> {
            evaluationTimes.incrementAndGet();
            return params;
        };

        now = 5;

        NoSpamLogger.log(mock, Level.INFO, 5, TimeUnit.NANOSECONDS, "TESTING {}", paramSupplier);
        assertEquals(1, evaluationTimes.get());
        Pair<String, Object[]> loggedMsg = logged.get(Level.INFO).remove();
        assertEquals("TESTING {}", loggedMsg.left);
        assertArrayEquals(params, loggedMsg.right);

        NoSpamLogger.log(mock, Level.INFO, 5, TimeUnit.NANOSECONDS, "TESTING {}", paramSupplier);
        assertEquals(1, evaluationTimes.get());
        assertTrue(logged.get(Level.INFO).isEmpty());

        now = 10;
        NoSpamLogger.log(mock, Level.INFO, 5, TimeUnit.NANOSECONDS, "TESTING {}", paramSupplier);
        assertEquals(2, evaluationTimes.get());
        loggedMsg = logged.get(Level.INFO).remove();
        assertEquals("TESTING {}", loggedMsg.left);
        assertArrayEquals(params, loggedMsg.right);
    }
}
