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

package org.apache.cassandra.db.virtual;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.ExceptionsTable.ExceptionRow;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.db.virtual.ExceptionsTable.EXCEPTIONS_TABLE_NAME;
import static org.apache.cassandra.utils.logging.AbstractVirtualTableAppender.getVirtualTable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExceptionsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @Test
    public void testExceptionInterceptionWhileVirtualTableIsNotRegistered()
    {
        doWithVTable(100, table ->
        {
            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException("my exception"));
            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException2("my exception2"));

            // register after treating exception, so it goes to pre-initialisation buffer first
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));

            // we have buffer populated with exceptions from time vtable was not initialized yet
            assertEquals(2, ExceptionsTable.preInitialisationBuffer.size());

            // flush buffer and populate table internally
            table.flush();

            assertTrue(ExceptionsTable.preInitialisationBuffer.isEmpty());

            ExceptionsTable.BoundedMap buffer = table.buffer;
            assertEquals(2, buffer.size());
        });
    }

    @Test
    public void testExceptionsTableWhileVirtualTableIsRegistered()
    {
        doWithVTable(100, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException("my exception"));
            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException2("my exception2"));

            assertTrue(ExceptionsTable.preInitialisationBuffer.isEmpty());
            ExceptionsTable.BoundedMap buffer = table.buffer;
            assertEquals(2, buffer.size());
        });
    }

    @Test
    public void testWrappedException()
    {
        doWithVTable(100, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            MyUncaughtException2 inner = new MyUncaughtException2("inner");
            MyUncaughtException myUncaughtException = new MyUncaughtException("outer", inner);

            JVMStabilityInspector.uncaughtException(currentThread(), myUncaughtException);

            assertTrue(ExceptionsTable.preInitialisationBuffer.isEmpty());
            ExceptionsTable.BoundedMap buffer = table.buffer;
            assertEquals(1, buffer.size());

            LinkedHashMap<String, ExceptionRow> entry = buffer.get(MyUncaughtException2.class.getName());
            assertNotNull(entry);
            ExceptionRow exceptionRow = entry.get(inner.getStackTrace()[0].toString());
            assertNotNull(exceptionRow);
            assertEquals("inner", exceptionRow.message);
            assertEquals(1, exceptionRow.count);
        });
    }

    @Test
    public void testDeduplication()
    {
        doWithVTable(100, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            // same type of exception

            MyUncaughtException exception1 = new MyUncaughtException("my exception");
            MyUncaughtException exception2 = new MyUncaughtException("my exception");
            MyUncaughtException2 exception3 = new MyUncaughtException2("my second uncaught exception");

            String firstLocation = ExceptionsTable.extractStacktrace(exception1.getStackTrace()).get(0);
            String secondLocation = ExceptionsTable.extractStacktrace(exception2.getStackTrace()).get(0);
            String thirdLocation = ExceptionsTable.extractStacktrace(exception3.getStackTrace()).get(0);

            JVMStabilityInspector.uncaughtException(currentThread(), exception1);
            JVMStabilityInspector.uncaughtException(currentThread(), exception1);
            JVMStabilityInspector.uncaughtException(currentThread(), exception2);
            JVMStabilityInspector.uncaughtException(currentThread(), exception3);

            ExceptionsTable.BoundedMap buffer = table.buffer;
            assertEquals(2, buffer.size());

            LinkedHashMap<String, ExceptionRow> exceptionRow = buffer.get(MyUncaughtException.class.getName());
            assertNotNull(exceptionRow);
            ExceptionRow firstLocationRow = exceptionRow.get(firstLocation);
            assertEquals("my exception", firstLocationRow.message);
            assertEquals(2, firstLocationRow.count);

            ExceptionRow secondLocationRow = exceptionRow.get(secondLocation);
            assertEquals("my exception", secondLocationRow.message);
            assertEquals(1, secondLocationRow.count);

            LinkedHashMap<String, ExceptionRow> exceptionRow2 = buffer.get(MyUncaughtException2.class.getName());

            assertNotNull(exceptionRow2);
            ExceptionRow firstLocationRow2 = exceptionRow2.get(thirdLocation);
            assertEquals("my second uncaught exception", firstLocationRow2.message);
            assertEquals(1, firstLocationRow2.count);

            List<UntypedResultSet.Row> rows = execute(format("SELECT * FROM %s.%s", KS_NAME, EXCEPTIONS_TABLE_NAME)).stream().collect(toList());
            assertEquals(3, rows.size());

            List<String> lastStacktraceException1 = rows.get(0).getList(ExceptionsTable.LAST_STACKTRACE_COLUMN_NAME, UTF8Type.instance);
            List<String> lastStacktraceException2 = rows.get(1).getList(ExceptionsTable.LAST_STACKTRACE_COLUMN_NAME, UTF8Type.instance);

            assertTrue(lastStacktraceException1.get(0).contains(ExceptionsTableTest.class.getName()));
            assertTrue(lastStacktraceException2.get(0).contains(ExceptionsTableTest.class.getName()));
        });
    }

    @Test
    public void testRemovalOfEntryWithOldestOccurence()
    {
        doWithVTable(4, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            MyUncaughtException2 myException2 = new MyUncaughtException2("my exception2");

            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException("my exception"));
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            JVMStabilityInspector.uncaughtException(currentThread(), myException2);
            JVMStabilityInspector.uncaughtException(currentThread(), myException2);

            JVMStabilityInspector.uncaughtException(currentThread(), new RuntimeException("some exception"));
            JVMStabilityInspector.uncaughtException(currentThread(), new IllegalStateException("some illegal state exception"));

            // we inserted 5th unique exception, so the oldest one, based on the occurence, will be removed
            // the first one inserted was MyUncaughtException with "my exception" message so that one will not be there anymore
            JVMStabilityInspector.uncaughtException(currentThread(), new IllegalArgumentException("some illegal state exception"));

            assertThat(table.buffer.size(), is(4));
            assertThat(table.buffer.get(MyUncaughtException.class.getName()), nullValue());

            assertThat(rows(format("SELECT * FROM %s.%s", KS_NAME, EXCEPTIONS_TABLE_NAME)).size(), is(4));

            JVMStabilityInspector.uncaughtException(currentThread(), new IllegalArgumentException("some illegal state exception"));

            assertThat(rows(format("SELECT * FROM %s.%s", KS_NAME, EXCEPTIONS_TABLE_NAME)).size(), is(4));
            assertNull(table.buffer.get(MyUncaughtException2.class.getName()));
        });
    }

    @Test
    public void testSelection()
    {
        doWithVTable(4, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            MyUncaughtException myUncaughtException = new MyUncaughtException("my exception");

            JVMStabilityInspector.uncaughtException(currentThread(), myUncaughtException);

            assertThat(rows(format("SELECT * FROM %s.%s WHERE exception_class = '%s'", KS_NAME, EXCEPTIONS_TABLE_NAME, MyUncaughtException.class.getName())), not(empty()));
            assertThat(rows(format("SELECT * FROM %s.%s WHERE exception_class = '%s'", KS_NAME, EXCEPTIONS_TABLE_NAME, IllegalArgumentException.class.getName())), empty());

            assertThat(rows(format("SELECT * FROM %s.%s WHERE exception_class = '%s' and exception_location = '%s'",
                                   KS_NAME, EXCEPTIONS_TABLE_NAME,
                                   MyUncaughtException.class.getName(),
                                   myUncaughtException.getStackTrace()[0])),
                       not(empty()));
        });
    }

    @Test
    public void testTruncation()
    {
        doWithVTable(4, table ->
        {
            // register before treating exception to avoid pre-initialisation buffer
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class,
                                                       KS_NAME,
                                                       EXCEPTIONS_TABLE_NAME);

            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtException("my exception"));

            assertThat(rows(format("SELECT * FROM %s.%s WHERE exception_class = '%s'",
                                   KS_NAME, EXCEPTIONS_TABLE_NAME,
                                   MyUncaughtException.class.getName())),
                       not(empty()));

            assertThat(rows(format("SELECT * FROM %s.%s WHERE exception_class = '%s'",
                                   KS_NAME, EXCEPTIONS_TABLE_NAME,
                                   MyUncaughtException.class.getName())),
                       not(empty()));

            execute(format("TRUNCATE %s.%s", KS_NAME, EXCEPTIONS_TABLE_NAME));

            assertThat(rows(format("SELECT * FROM %s.%s;", KS_NAME, EXCEPTIONS_TABLE_NAME)), empty());
        });
    }

    @Test
    public void testEmptyStacktrace()
    {
        doWithVTable(4, table ->
        {
            // register after treating exception, so it goes to pre-initialisation buffer first
            VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
            ExceptionsTable.INSTANCE = getVirtualTable(ExceptionsTable.class, KS_NAME, EXCEPTIONS_TABLE_NAME);

            JVMStabilityInspector.uncaughtException(currentThread(), new MyUncaughtExceptionWithoutStacktrace("my exception"));

            ExceptionsTable.BoundedMap buffer = table.buffer;
            assertEquals(1, buffer.size());

            LinkedHashMap<String, ExceptionRow> partition = buffer.get(MyUncaughtExceptionWithoutStacktrace.class.getName());
            ExceptionRow clustering = partition.get("unknown");
            assertNotNull(clustering);
            assertEquals(1, clustering.count);
            assertEquals("my exception", clustering.message);
        });
    }

    private List<UntypedResultSet.Row> rows(String query)
    {
        return execute(query).stream().collect(toList());
    }

    private void doWithVTable(int maxSize, Consumer<ExceptionsTable> consumer)
    {
        ExceptionsTable table = new ExceptionsTable(KS_NAME, maxSize);
        VirtualKeyspace keyspace = new VirtualKeyspace(KS_NAME, ImmutableList.of(table));

        try
        {
            consumer.accept(table);
        }
        finally
        {
            ExceptionsTable.preInitialisationBuffer.clear();
            VirtualKeyspaceRegistry.instance.unregister(keyspace);
            ExceptionsTable.INSTANCE = null;
        }
    }

    public static class MyUncaughtException extends Throwable
    {
        public MyUncaughtException(String message)
        {
            super(message);
        }

        public MyUncaughtException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    public static class MyUncaughtException2 extends Throwable
    {
        public MyUncaughtException2(String message)
        {
            super(message);
        }
    }

    public static class MyUncaughtExceptionWithoutStacktrace extends Throwable
    {
        public MyUncaughtExceptionWithoutStacktrace(String message)
        {
            super(message, null, false, false);
        }
    }
}
