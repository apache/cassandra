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
package org.apache.cassandra.utils;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.cassandra.utils.Clock.Global;

/**
 * Logging that limits each log statement to firing based on time since the statement last fired.
 *
 * Every logger has a unique timer per statement. Minimum time between logging is set for each statement
 * the first time it is used and a subsequent attempt to request that statement with a different minimum time will
 * result in the original time being used. No warning is provided if there is a mismatch.
 *
 * If the statement is cached and used to log directly then only a volatile read will be required in the common case.
 * If the Logger is cached then there is a single concurrent hash map lookup + the volatile read.
 * If neither the logger nor the statement is cached then it is two concurrent hash map lookups + the volatile read.
 *
 */
public class NoSpamLogger
{
    /**
     * Levels for programmatically specifying the severity of a log statement
     */
    public enum Level
    {
        INFO, WARN, ERROR
    }

    @VisibleForTesting
    public interface Clock
    {
        long nanoTime();
    }

    private static Clock CLOCK = Global::nanoTime;

    @VisibleForTesting
    public static void unsafeSetClock(Clock clock)
    {
        CLOCK = clock;
    }

    public class NoSpamLogStatement extends AtomicLong
    {
        private static final long serialVersionUID = 1L;

        private final String statement;
        private final long minIntervalNanos;

        public NoSpamLogStatement(String statement, long minIntervalNanos)
        {
            super(Long.MIN_VALUE);
            this.statement = statement;
            this.minIntervalNanos = minIntervalNanos;
        }

        private boolean shouldLog(long nowNanos)
        {
            long expected = get();
            return nowNanos >= expected && compareAndSet(expected, nowNanos + minIntervalNanos);
        }

        public boolean log(Level l, long nowNanos, Supplier<Object[]> objects)
        {
            if (!shouldLog(nowNanos)) return false;
            return logNoCheck(l, objects.get());
        }

        public boolean log(Level l, long nowNanos, Object... objects)
        {
            if (!shouldLog(nowNanos)) return false;
            return logNoCheck(l, objects);
        }

        private boolean logNoCheck(Level l, Object... objects)
        {
            switch (l)
            {
                case INFO:
                    wrapped.info(statement, objects);
                    break;
                case WARN:
                    wrapped.warn(statement, objects);
                    break;
                case ERROR:
                    wrapped.error(statement, objects);
                    break;
                default:
                    throw new AssertionError();
            }
            return true;
        }

        public boolean info(long nowNanos, Object... objects)
        {
            return NoSpamLogStatement.this.log(Level.INFO, nowNanos, objects);
        }

        public boolean info(Object... objects)
        {
            return NoSpamLogStatement.this.info(CLOCK.nanoTime(), objects);
        }

        public boolean warn(long nowNanos, Object... objects)
        {
            return NoSpamLogStatement.this.log(Level.WARN, nowNanos, objects);
        }

        public boolean warn(Object... objects)
        {
            return NoSpamLogStatement.this.warn(CLOCK.nanoTime(), objects);
        }

        public boolean error(long nowNanos, Object... objects)
        {
            return NoSpamLogStatement.this.log(Level.ERROR, nowNanos, objects);
        }

        public boolean error(Object... objects)
        {
            return NoSpamLogStatement.this.error(CLOCK.nanoTime(), objects);
        }
    }

    private static final NonBlockingHashMap<Logger, NoSpamLogger> wrappedLoggers = new NonBlockingHashMap<>();

    @VisibleForTesting
    static void clearWrappedLoggersForTest()
    {
        wrappedLoggers.clear();
    }

    public static NoSpamLogger getLogger(Logger logger, long minInterval, TimeUnit unit)
    {
        NoSpamLogger wrapped = wrappedLoggers.get(logger);
        if (wrapped == null)
        {
            wrapped = new NoSpamLogger(logger, minInterval, unit);
            NoSpamLogger temp = wrappedLoggers.putIfAbsent(logger, wrapped);
            if (temp != null)
                wrapped = temp;
        }
        return wrapped;
    }

    public static boolean log(Logger logger, Level level, long minInterval, TimeUnit unit, String message, Object... objects)
    {
        return log(logger, level, message, minInterval, unit, CLOCK.nanoTime(), message, objects);
    }

    public static boolean log(Logger logger, Level level, String key, long minInterval, TimeUnit unit, String message, Object... objects)
    {
        return log(logger, level, key, minInterval, unit, CLOCK.nanoTime(), message, objects);
    }

    public static boolean log(Logger logger, Level level, String key, long minInterval, TimeUnit unit, long nowNanos, String message, Object... objects)
    {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
        NoSpamLogStatement statement = wrapped.getStatement(key, message);
        return statement.log(level, nowNanos, objects);
    }

    public static boolean log(Logger logger, Level level, long minInterval, TimeUnit unit, String message, Supplier<Object[]> objects)
    {
        return log(logger, level, message, minInterval, unit, CLOCK.nanoTime(), message, objects);
    }

    public static boolean log(Logger logger, Level level, String key, long minInterval, TimeUnit unit, String message, Supplier<Object[]> objects)
    {
        return log(logger, level, key, minInterval, unit, CLOCK.nanoTime(), message, objects);
    }

    public static boolean log(Logger logger, Level level, String key, long minInterval, TimeUnit unit, long nowNanos, String message, Supplier<Object[]> objects)
    {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
        NoSpamLogStatement statement = wrapped.getStatement(key, message);
        return statement.log(level, nowNanos, objects);
    }

    public static NoSpamLogStatement getStatement(Logger logger, String message, long minInterval, TimeUnit unit)
    {
        NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
        return wrapped.getStatement(message);
    }

    private final Logger wrapped;
    private final long minIntervalNanos;
    private final NonBlockingHashMap<String, NoSpamLogStatement> lastMessage = new NonBlockingHashMap<>();

    private NoSpamLogger(Logger wrapped, long minInterval, TimeUnit timeUnit)
    {
        this.wrapped = wrapped;
        minIntervalNanos = timeUnit.toNanos(minInterval);
    }

    public boolean info(long nowNanos, String s, Object... objects)
    {
        return NoSpamLogger.this.log( Level.INFO, s, nowNanos, objects);
    }

    public boolean info(String s, Object... objects)
    {
        return NoSpamLogger.this.info(CLOCK.nanoTime(), s, objects);
    }

    public boolean warn(long nowNanos, String s, Object... objects)
    {
        return NoSpamLogger.this.log( Level.WARN, s, nowNanos, objects);
    }

    public boolean warn(String s, Object... objects)
    {
        return NoSpamLogger.this.warn(CLOCK.nanoTime(), s, objects);
    }

    public boolean error(long nowNanos, String s, Object... objects)
    {
        return NoSpamLogger.this.log( Level.ERROR, s, nowNanos, objects);
    }

    public boolean error(String s, Object... objects)
    {
        return NoSpamLogger.this.error(CLOCK.nanoTime(), s, objects);
    }

    public boolean log(Level l, String s, long nowNanos, Object... objects)
    {
        return NoSpamLogger.this.getStatement(s, minIntervalNanos).log(l, nowNanos, objects);
    }

    public NoSpamLogStatement getStatement(String s)
    {
        return NoSpamLogger.this.getStatement(s, minIntervalNanos);
    }

    public NoSpamLogStatement getStatement(String key, String s)
    {
        return NoSpamLogger.this.getStatement(key, s, minIntervalNanos);
    }

    public NoSpamLogStatement getStatement(String s, long minInterval, TimeUnit unit)
    {
        return NoSpamLogger.this.getStatement(s, unit.toNanos(minInterval));
    }

    public NoSpamLogStatement getStatement(String s, long minIntervalNanos)
    {
        return getStatement(s, s, minIntervalNanos);
    }

    public NoSpamLogStatement getStatement(String key, String s, long minIntervalNanos)
    {
        NoSpamLogStatement statement = lastMessage.get(key);
        if (statement == null)
        {
            statement = new NoSpamLogStatement(s, minIntervalNanos);
            NoSpamLogStatement temp = lastMessage.putIfAbsent(key, statement);
            if (temp != null)
                statement = temp;
        }
        return statement;
    }
}
