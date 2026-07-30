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

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;
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

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.utils.NoSpamLogger.Level;
import org.apache.cassandra.utils.NoSpamLogger.NoDuplicateSpamLogStatement;
import org.apache.cassandra.utils.NoSpamLogger.NoSpamLogStatement;

import static org.apache.cassandra.config.CassandraRelevantProperties.NOSPAM_LOGGER_MAX_STATEMENTS_PER_LOGGER;
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
        try (WithProperties properties = new WithProperties().set(NOSPAM_LOGGER_MAX_STATEMENTS_PER_LOGGER,
                                                                  String.valueOf(maxStatementsPerLogger)))
        {
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

    // BELOW TESTS WERE AUTHORED BY CLAUDE
    // ---------------------------------------------------------------------------------------------------------------
    // NoDuplicateSpamLogStatement: per-identity rate limiting. The state it keeps to recognise a duplicate must be
    // bounded (it is fed by error storms), and recognising a duplicate must be cheap (it is on the storm path).
    // ---------------------------------------------------------------------------------------------------------------

    private static final long INTERVAL_NANOS = 100;
    /**
     * Upper bound we are willing to see retained. The implementation's own cap is 1024; we assert against a bound
     * that is generous but still independent of {@link #DISTINCT_IDS}, so the test states "bounded", not "exactly
     * 1024", and does not have to be edited if the cap is retuned.
     */
    private static final int RETAINED_BOUND = 4096;
    private static final int DISTINCT_IDS = 20_000;

    private static Map<?, ?> lastLoggedOf(NoDuplicateSpamLogStatement statement) throws Exception
    {
        // Reflection, deliberately: the class exposes no accessor for its suppression state, and the alternative
        // (asserting only on behaviour) cannot say *how many* entries were retained, which is the whole finding.
        // The behavioural assertion in the same test is the primary one; this is here so that the failure message
        // names the actual defect (a map of N entries) rather than only its symptom.
        Field field = NoDuplicateSpamLogStatement.class.getDeclaredField("lastLogged");
        field.setAccessible(true);
        return (Map<?, ?>) field.get(statement);
    }

    /**
     * Pins: {@code NoDuplicateSpamLogStatement}'s duplicate-suppression state is bounded.
     *
     * Eviction is driven by the very event it is meant to bound: entries are pruned only from inside a call that
     * passes the gate (i.e. only while logging continues), only once per interval, and only if already expired.
     * A burst of distinct identities followed by quiescence therefore used to retain one entry per identity for the
     * lifetime of the process - the number of identities is not bounded by code paths (line numbers x cause chains
     * x suppressed sets), and this statement is fed by AccordAgent's uncaught-exception path, i.e. a storm.
     *
     * The clock is held still for the whole test, so no entry can expire and the prune loop can remove nothing:
     * the only thing that can shrink the map is a hard size cap. Two assertions, in order:
     *  - behavioural (public API only): an identity logged before the cap was reached is loggable again, because a
     *    bounded map must have forgotten it. Without a cap it stays suppressed for ever.
     *  - state: the retained entry count does not grow with the number of distinct identities.
     *
     * If the cap is removed, both fail; the first with "still suppressed", the second with the actual map size.
     */
    @Test
    public void testNoDuplicateSpamLoggerStateIsBounded() throws Exception
    {
        now = 5;
        NoDuplicateSpamLogStatement nospam = new NoDuplicateSpamLogStatement(mock, statement, INTERVAL_NANOS, TimeUnit.NANOSECONDS);

        long firstId = 0;
        assertTrue(nospam.warn(firstId, param));
        assertFalse("an identity repeated inside the interval must be suppressed", nospam.warn(firstId, param));
        assertLoggedSizes(0, 1, 0);

        // a burst of distinct identities, with the clock frozen: nothing expires, so nothing can be pruned
        for (long id = 1; id <= DISTINCT_IDS; ++id)
            assertTrue("a never-before-seen identity must always be logged (id " + id + ')', nospam.warn(id, param));
        assertLoggedSizes(0, 1 + DISTINCT_IDS, 0);

        assertTrue("identity " + firstId + " is still suppressed after " + DISTINCT_IDS + " further distinct " +
                   "identities were logged with the clock frozen: nothing evicted it, i.e. the suppression state has " +
                   "no size cap and grows for the lifetime of the process (only expired entries are pruned, and only " +
                   "on a call that logs)",
                   nospam.warn(firstId, param));

        int retained = lastLoggedOf(nospam).size();
        assertTrue("lastLogged retained " + retained + " entries after " + (DISTINCT_IDS + 1) + " distinct " +
                   "identities with the clock frozen (expected at most " + RETAINED_BOUND + "): the duplicate-" +
                   "suppression map is unbounded",
                   retained <= RETAINED_BOUND);
    }

    /**
     * Pins: bounding the state did not break what the state is for. Same identity inside the interval is logged
     * once, distinct identities are independent, and the interval still applies to each identity separately.
     *
     * Deliberately stays well below the cap (and below the prune threshold of 32) so that it is only about the
     * suppression contract. A cap that cleared the map too eagerly, or a gate that ignored the identity, would show
     * up here as a duplicate message.
     */
    @Test
    public void testNoDuplicateSpamLoggerSuppression()
    {
        now = 5;
        NoDuplicateSpamLogStatement nospam = new NoDuplicateSpamLogStatement(mock, statement, INTERVAL_NANOS, TimeUnit.NANOSECONDS);

        assertTrue(nospam.warn(1L, param));
        assertFalse(nospam.warn(1L, param));
        assertFalse(nospam.warn(1L, param));
        assertLoggedSizes(0, 1, 0);

        assertTrue("a distinct identity must not be suppressed by another identity's interval", nospam.warn(2L, param));
        assertFalse(nospam.warn(2L, param));
        assertLoggedSizes(0, 2, 0);

        // one nanosecond before identity 1's interval expires
        now = 5 + INTERVAL_NANOS - 1;
        assertFalse("the interval must still be enforced per identity", nospam.warn(1L, param));
        assertLoggedSizes(0, 2, 0);

        now = 5 + INTERVAL_NANOS;
        assertTrue("the identity must be logged again once its interval has elapsed", nospam.warn(1L, param));
        assertFalse(nospam.warn(1L, param));
        assertLoggedSizes(0, 3, 0);

        // and the arguments still reach the wrapped logger unchanged
        Pair<String, Object[]> p = logged.get(Level.WARN).poll();
        assertNotNull(p);
        assertEquals(statement, p.left);
        assertArrayEquals(new Object[]{ param }, p.right);
    }

    /**
     * Pins: the cheap gate is still discriminating enough to tell distinct exception *shapes* apart - it walks the
     * cause chain, it is not just the type of the outermost throwable. Otherwise the cost fix would have bought
     * itself by swallowing genuinely different failures under one message per interval.
     *
     * Version-neutral by construction (it holds no opinion on stack traces), so it is a guard on the gate's
     * fidelity rather than a detector; the detector for the gate itself is
     * {@link #testSuppressedExceptionDoesNotWalkTheStackTrace}.
     */
    @Test
    public void testDistinctCauseChainsAreLoggedSeparately()
    {
        now = 5;
        NoDuplicateSpamLogStatement nospam = new NoDuplicateSpamLogStatement(mock, statement, INTERVAL_NANOS, TimeUnit.NANOSECONDS);

        Throwable ise = new RuntimeException("a", new IllegalStateException("inner"));
        assertTrue(nospam.warn(ise));
        assertTrue("a different cause type is a different failure and must be logged",
                   nospam.warn(new RuntimeException("a", new IllegalArgumentException("inner"))));
        assertLoggedSizes(0, 2, 0);

        // differing only at the third level of the cause chain
        assertTrue(nospam.warn(new RuntimeException("b", new RuntimeException("m", new IllegalStateException("deep")))));
        assertTrue("the gate must walk the whole cause chain, not only the outermost throwable",
                   nospam.warn(new RuntimeException("b", new RuntimeException("m", new IllegalArgumentException("deep")))));
        assertLoggedSizes(0, 4, 0);

        // ... and each of those shapes is now suppressed for the interval
        assertFalse("a throwable already logged in this interval must be suppressed", nospam.warn(ise));
        assertLoggedSizes(0, 4, 0);
    }
}
