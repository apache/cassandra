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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.helpers.SubstituteLogger;

import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
   static long tickerTime;

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        NoSpamLogger.unsafeSetClock(() -> now);
        NoSpamLogger.TICKER = () -> tickerTime;
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

    /**
     * Test that the {@link NoSpamLogStatement} cache is bounded and doesn't grow beyond max_statements_per_logger.
     * This prevents memory exhaustion from dynamic log messages (e.g., queries with unique strings).
     */
    @Test
    public void testNoSpamLogStatementCacheBounded()
    {
        int maxStatementsPerLogger = 10;
        System.setProperty("cassandra.nospam_logger.max_statements_per_logger", String.valueOf(maxStatementsPerLogger));
        try
        {
            NoSpamLogger.clearWrappedLoggersForTest();
            now = 5;
            NoSpamLogger logger = NoSpamLogger.getLogger(mock, 5, TimeUnit.NANOSECONDS);

            // Create more unique log statements than the cache can hold
            int numberOfLogStatements = (int) (maxStatementsPerLogger * 1.5);
            for (int i = 0; i < numberOfLogStatements; i++)
            {
                String uniqueStatement = "statement" + i + "{}";
                assertTrue("First occurrence of statement " + i + " should succeed",
                          logger.info(uniqueStatement, param));
                now += 10; // Advance time so each statement can log
            }

            assertEquals(numberOfLogStatements, logged.get(Level.INFO).size());

            // Force cache cleanup to ensure eviction has completed
            logger.cleanUpStatementsForTest();

            // Verify the cache size is bounded to the configured maximum
            assertTrue("Cache size should be at most " + maxStatementsPerLogger, logger.getStatementsCount() <= maxStatementsPerLogger);
        }
        finally
        {
            System.clearProperty("cassandra.nospam_logger.max_statements_per_logger");
            NoSpamLogger.clearWrappedLoggersForTest();
        }
    }

    /**
     * Test that log statements expire after the configured inactivity period.
     */
    @Test
    public void testNoSpamLogStatementsCacheTimeBasedEviction()
    {
        try
        {
            int minIntervalInseconds = 10;
            NoSpamLogger.clearWrappedLoggersForTest();
            now = 0;
            tickerTime = 0;
            NoSpamLogger logger = NoSpamLogger.getLogger(mock, minIntervalInseconds, TimeUnit.SECONDS);

            assertTrue(logger.info("test{}", param));
            assertEquals(1, logged.get(Level.INFO).size());
            assertEquals("Cache should contain 1 statement", 1, logger.getStatementsCount());

            // Try to log again immediately - should be rate-limited
            assertFalse(logger.info("test{}", param));
            assertEquals(1, logged.get(Level.INFO).size());
            assertEquals("Cache should still contain 1 statement", 1, logger.getStatementsCount());

            // Advance BOTH clocks by more than `minIntervalInseconds` seconds
            // `now` is used for rate limiting (NoSpamLogger.CLOCK)
            // `tickerTime` is used for cache expiration (Caffeine's Ticker)
            long advanceTime = TimeUnit.SECONDS.toNanos(minIntervalInseconds + 1);
            now += advanceTime;
            tickerTime += advanceTime;

            // Trigger cache cleanup to process expired entries
            logger.cleanUpStatementsForTest();

            // Verify the statement was evicted from cache
            assertEquals("Cache should be empty after expiration", 0, logger.getStatementsCount());

            // The statement should have expired from cache, so it should log again
            assertTrue("Statement should have expired and can log again",
                      logger.info("test{}", param));
            assertEquals(2, logged.get(Level.INFO).size());
            assertEquals("Cache should contain 1 statement again", 1, logger.getStatementsCount());
        }
        finally
        {
            NoSpamLogger.clearWrappedLoggersForTest();
        }
    }

    /**
     * Test that NoSpamLogger instances are cached and reused.
     * This test verifies that getting the same logger returns the cached instance,
     * and that clearing the cache creates new instances.
     */
    @Test
    public void testNoSpamLoggerCaching()
    {
        NoSpamLogger.clearWrappedLoggersForTest();
        now = 0;

        // Create multiple unique logger instances
        Logger logger1 = new SubstituteLogger("testLogger1", null, true)
        {
            @Override
            public void info(String statement, Object... args)
            {
                logged.get(Level.INFO).offer(Pair.create(statement, args));
            }

            @Override
            public int hashCode()
            {
                return System.identityHashCode(this);
            }

            @Override
            public boolean equals(Object o)
            {
                return this == o;
            }
        };

        Logger logger2 = new SubstituteLogger("testLogger2", null, true)
        {
            @Override
            public void info(String statement, Object... args)
            {
                logged.get(Level.INFO).offer(Pair.create(statement, args));
            }

            @Override
            public int hashCode()
            {
                return System.identityHashCode(this);
            }

            @Override
            public boolean equals(Object o)
            {
                return this == o;
            }
        };

        // Get NoSpamLogger instances - these should be cached
        NoSpamLogger nsl1 = NoSpamLogger.getLogger(logger1, 5, TimeUnit.NANOSECONDS);
        NoSpamLogger nsl2 = NoSpamLogger.getLogger(logger2, 5, TimeUnit.NANOSECONDS);

        assertTrue(nsl1.info("test{}", param));
        assertTrue(nsl2.info("test{}", param));
        assertEquals(2, logged.get(Level.INFO).size());

        // Verify that getting the same logger returns the cached instance
        NoSpamLogger nsl1Again = NoSpamLogger.getLogger(logger1, 5, TimeUnit.NANOSECONDS);
        assertSame("Should return cached instance", nsl1, nsl1Again);

        // Forcefully clear all cached loggers
        NoSpamLogger.clearWrappedLoggersForTest();

        // Getting the logger again should create a new instance
        NoSpamLogger nsl1New = NoSpamLogger.getLogger(logger1, 5, TimeUnit.NANOSECONDS);
        assertNotSame("Should create new instance after cache clear", nsl1, nsl1New);

        // Verify the new instance works correctly
        assertTrue("New logger instance should log immediately", nsl1New.info("test{}", param));
        assertEquals(3, logged.get(Level.INFO).size());
    }

    /**
     * Test that the NoSpamLogStatement cache uses custom per-entry expiry based on each logger's minIntervalNanos.
     * This test verifies that different NoSpamLogger instances with different intervals result in
     * different expiry times for their cached statements.
     */
    @Test
    public void testNoSpamLogStatementCacheCustomExpiry()
    {
        NoSpamLogger.clearWrappedLoggersForTest();
        now = 0;
        tickerTime = 0;

        // Create three NoSpamLogger instances with different intervals
        int[] intervals = { 2, 5, 10 };
        NoSpamLogger[] loggers = new NoSpamLogger[intervals.length];
        int logMessagesPerLogger = 3;
        for (int i = 0; i < intervals.length; i++)
        {
            // Create a unique Logger instance for each interval to get separate NoSpamLogger instances
            Logger testLogger = new SubstituteLogger("testLogger" + i, null, true)
            {
                @Override
                public void info(String statement, Object... args)
                {
                    logged.get(Level.INFO).offer(Pair.create(statement, args));
                }

                @Override
                public int hashCode()
                {
                    return System.identityHashCode(this);
                }

                @Override
                public boolean equals(Object o)
                {
                    return this == o;
                }
            };

            loggers[i] = NoSpamLogger.getLogger(testLogger, intervals[i], TimeUnit.SECONDS);

            // Log 3 messages from each logger
            for (int j = 1; j <= logMessagesPerLogger; j++)
            {
                assertTrue(loggers[i].info("message" + j));
                now += intervals[i] * 1_000_000_000L + 1; // Advance past the interval to allow next log
            }
            assertEquals(logMessagesPerLogger, loggers[i].getStatementsCount());
        }

        assertEquals(logMessagesPerLogger * intervals.length, logged.get(Level.INFO).size());

        // Test expiry at different time points
        // Entries were created at tickerTime=0, so they expire at their interval time
        int[] checkTimes = new int[intervals.length];
        for (int i = 0; i < intervals.length; i++)
        {
            // Set check time to 1 second after expiry (entries expire at interval seconds)
            checkTimes[i] = intervals[i] + 1;
        }

        for (int timeIdx = 0; timeIdx < checkTimes.length; timeIdx++)
        {
            tickerTime = TimeUnit.SECONDS.toNanos(checkTimes[timeIdx]);

            for (int i = 0; i < loggers.length; i++)
            {
                loggers[i].cleanUpStatementsForTest();

                // Entries expire at (creation_time + interval), created at time 0
                // So they expire when tickerTime > interval
                int expected = (intervals[i] < checkTimes[timeIdx]) ? 0 : logMessagesPerLogger;
                assertEquals(String.format("After %ds, %d-second logger should have %d statements",
                                           checkTimes[timeIdx], intervals[i], expected),
                             expected, loggers[i].getStatementsCount());
            }
        }
    }
}
