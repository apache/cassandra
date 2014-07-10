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
package org.apache.cassandra.cql3;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Base class for CQL tests.
 */
public abstract class CQLTester
{
    protected static final Logger logger = LoggerFactory.getLogger(CQLTester.class);

    private static final String KEYSPACE = "cql_test_keyspace";
    private static final boolean USE_PREPARED_VALUES = Boolean.valueOf(System.getProperty("cassandra.test.use_prepared", "true"));
    private static final AtomicInteger seqNumber = new AtomicInteger();

    static
    {
        // Once per-JVM is enough
        SchemaLoader.prepareServer();
    }

    private String currentTable;
    private final Set<String> currentTypes = new HashSet<>();

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", KEYSPACE));
    }

    @AfterClass
    public static void tearDownClass()
    {
    }

    @After
    public void afterTest() throws Throwable
    {
        if (currentTable == null)
            return;

        final String tableToDrop = currentTable;
        final Set<String> typesToDrop = currentTypes.isEmpty() ? Collections.emptySet() : new HashSet(currentTypes);
        currentTable = null;
        currentTypes.clear();

        // We want to clean up after the test, but dropping a table is rather long so just do that asynchronously
        StorageService.tasks.execute(new Runnable()
        {
            public void run()
            {
                try
                {
                    schemaChange(String.format("DROP TABLE %s.%s", KEYSPACE, tableToDrop));

                    for (String typeName : typesToDrop)
                        schemaChange(String.format("DROP TYPE %s.%s", KEYSPACE, typeName));

                    // Dropping doesn't delete the sstables. It's not a huge deal but it's cleaner to cleanup after us
                    // Thas said, we shouldn't delete blindly before the SSTableDeletingTask for the table we drop
                    // have run or they will be unhappy. Since those taks are scheduled on StorageService.tasks and that's
                    // mono-threaded, just push a task on the queue to find when it's empty. No perfect but good enough.

                    final CountDownLatch latch = new CountDownLatch(1);
                    StorageService.tasks.execute(new Runnable()
                    {
                            public void run()
                            {
                                latch.countDown();
                            }
                    });
                    latch.await(2, TimeUnit.SECONDS);

                    removeAllSSTables(KEYSPACE, tableToDrop);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static void removeAllSSTables(String ks, String table)
    {
        // clean up data directory which are stored as data directory/keyspace/data files
        for (File d : Directories.getKSChildDirectories(ks))
        {
            if (d.exists() && d.getName().contains(table))
                FileUtils.deleteRecursive(d);
        }
    }

    protected String createType(String query)
    {
        String typeName = "type_" + seqNumber.getAndIncrement();
        String fullQuery = String.format(query, KEYSPACE + "." + typeName);
        currentTypes.add(typeName);
        logger.info(fullQuery);
        schemaChange(fullQuery);
        return typeName;
    }

    protected void createTable(String query)
    {
        currentTable = "table_" + seqNumber.getAndIncrement();
        String fullQuery = String.format(query, KEYSPACE + "." + currentTable);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    protected void createIndex(String query)
    {
        String fullQuery = String.format(query, KEYSPACE + "." + currentTable);
        logger.info(fullQuery);
        schemaChange(fullQuery);
    }

    private static void schemaChange(String query)
    {
        try
        {
            // executeOnceInternal don't work for schema changes
            QueryProcessor.executeOnceInternal(query);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error setting schema for test (query was: " + query + ")", e);
        }
    }

    protected UntypedResultSet execute(String query, Object... values) throws Throwable
    {
        if (currentTable == null)
            throw new RuntimeException("You must create a table first with createTable");

        try
        {
            query = String.format(query, KEYSPACE + "." + currentTable);

            UntypedResultSet rs;
            if (USE_PREPARED_VALUES)
            {
                logger.info("Executing: {} with values {}", query, formatAllValues(values));
                rs = QueryProcessor.executeOnceInternal(query, transformValues(values));
            }
            else
            {
                query = replaceValues(query, values);
                logger.info("Executing: {}", query);
                rs = QueryProcessor.executeOnceInternal(query);
            }
            if (rs != null)
                logger.info("Got {} rows", rs.size());
            return rs;
        }
        catch (RuntimeException e)
        {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            logger.info("Got error: {}", cause.getMessage() == null ? cause.toString() : cause.getMessage());
            throw cause;
        }
    }

    protected void assertRows(UntypedResultSet result, Object[]... rows)
    {
        if (result == null)
        {
            if (rows.length > 0)
                Assert.fail(String.format("No rows returned by query but %d expected", rows.length));
            return;
        }

        List<ColumnSpecification> meta = result.metadata();
        Iterator<UntypedResultSet.Row> iter = result.iterator();
        int i = 0;
        while (iter.hasNext() && i < rows.length)
        {
            Object[] expected = rows[i++];
            UntypedResultSet.Row actual = iter.next();

            Assert.assertEquals(String.format("Invalid number of (expected) values provided for row %d", i), meta.size(), expected.length);

            for (int j = 0; j < meta.size(); j++)
            {
                ColumnSpecification column = meta.get(j);
                Object expectedValue = expected[j];
                ByteBuffer expectedByteValue = makeByteBuffer(expected[j], (AbstractType)column.type);
                ByteBuffer actualValue = actual.getBytes(column.name.toString());

                if (!Objects.equal(expectedByteValue, actualValue))
                    Assert.fail(String.format("Invalid value for row %d column %d (%s), expected <%s> but got <%s>",
                                              i, j, column.name, formatValue(expectedByteValue, column.type), formatValue(actualValue, column.type)));
            }
        }

        if (iter.hasNext())
        {
            while (iter.hasNext())
            {
                iter.next();
                i++;
            }
            Assert.fail(String.format("Got less rows than expected. Expected %d but got %d.", rows.length, i));
        }

        Assert.assertTrue(String.format("Got more rows than expected. Expected %d but got %d", rows.length, i), i == rows.length);
    }

    protected void assertAllRows(Object[]... rows) throws Throwable
    {
        assertRows(execute("SELECT * FROM %s"), rows);
    }

    protected Object[] row(Object... expected)
    {
        return expected;
    }

    protected void assertEmpty(UntypedResultSet result) throws Throwable
    {
        if (result != null && result.size() != 0)
            throw new InvalidRequestException(String.format("Expected empty result but got %d rows", result.size()));
    }

    protected void assertInvalid(String query, Object... values) throws Throwable
    {
        try
        {
            execute(query, values);
            Assert.fail("Query should be invalid but no error was thrown. Query is: " + query);
        }
        catch (SyntaxException | InvalidRequestException e)
        {
            // This is what we expect
        }
    }

    private static String replaceValues(String query, Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        int last = 0;
        int i = 0;
        int idx;
        while ((idx = query.indexOf('?', last)) > 0)
        {
            if (i >= values.length)
                throw new IllegalArgumentException(String.format("Not enough values provided. The query has at least %d variables but only %d values provided", i, values.length));

            sb.append(query.substring(last, idx));

            Object value = values[i++];

            // When we have a .. IN ? .., we use a list for the value because that's what's expected when the value is serialized.
            // When we format as string however, we need to special case to use parenthesis. Hackish but convenient.
            if (idx >= 3 && value instanceof List && query.substring(idx - 3, idx).equalsIgnoreCase("IN "))
            {
                List l = (List)value;
                sb.append("(");
                for (int j = 0; j < l.size(); j++)
                {
                    if (j > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(j)));
                }
                sb.append(")");
            }
            else
            {
                sb.append(formatForCQL(value));
            }
            last = idx + 1;
        }
        sb.append(query.substring(last));
        return sb.toString();
    }

    // We're rellly only returning ByteBuffers but this make the type system happy
    private static Object[] transformValues(Object[] values)
    {
        // We could partly rely on QueryProcessor.executeOnceInternal doing type conversion for us, but
        // it would complain with ClassCastException if we pass say a string where an int is excepted (since
        // it bases conversion on what the value should be, not what it is). For testing, we sometimes
        // want to pass value of the wrong type and assert that this properly raise an InvalidRequestException
        // and executeOnceInternal goes into way. So instead, we pre-convert everything to bytes here base
        // on the value.
        // Besides, we need to handle things like TupleValue that executeOnceInternal don't know about.

        Object[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            if (value == null)
            {
                buffers[i] = null;
                continue;
            }

            buffers[i] = typeFor(value).decompose(serializeTuples(value));
        }
        return buffers;
    }

    private static Object serializeTuples(Object value)
    {
        if (value instanceof TupleValue)
        {
            return ((TupleValue)value).toByteBuffer();
        }

        // We need to reach inside collections for TupleValue and transform them to ByteBuffer
        // since otherwise the decompose method of the collection AbstractType won't know what
        // to do with them
        if (value instanceof List)
        {
            List l = (List)value;
            List n = new ArrayList(l.size());
            for (Object o : l)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            Set n = new LinkedHashSet(s.size());
            for (Object o : s)
                n.add(serializeTuples(o));
            return n;
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            Map n = new LinkedHashMap(m.size());
            for (Object entry : m.entrySet())
                n.put(serializeTuples(((Map.Entry)entry).getKey()), serializeTuples(((Map.Entry)entry).getValue()));
            return n;
        }
        return value;
    }

    private static String formatAllValues(Object[] values)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(formatForCQL(values[i]));
        }
        sb.append("]");
        return sb.toString();
    }

    private static String formatForCQL(Object value)
    {
        if (value == null)
            return "null";

        if (value instanceof TupleValue)
            return ((TupleValue)value).toCQLString();

        // We need to reach inside collections for TupleValue. Besides, for some reason the format
        // of collection that CollectionType.getString gives us is not at all 'CQL compatible'
        if (value instanceof Collection)
        {
            StringBuilder sb = new StringBuilder();
            if (value instanceof List)
            {
                List l = (List)value;
                sb.append("[");
                for (int i = 0; i < l.size(); i++)
                {
                    if (i > 0)
                        sb.append(", ");
                    sb.append(formatForCQL(l.get(i)));
                }
                sb.append("[");
            }
            else if (value instanceof Set)
            {
                Set s = (Set)value;
                sb.append("{");
                Iterator iter = s.iterator();
                while (iter.hasNext())
                {
                    sb.append(formatForCQL(iter.next()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            else
            {
                Map m = (Map)value;
                sb.append("{");
                Iterator iter = m.entrySet().iterator();
                while (iter.hasNext())
                {
                    Map.Entry entry = (Map.Entry)iter.next();
                    sb.append(formatForCQL(entry.getKey())).append(": ").append(formatForCQL(entry.getValue()));
                    if (iter.hasNext())
                        sb.append(", ");
                }
                sb.append("}");
            }
            return sb.toString();
        }

        AbstractType type = typeFor(value);
        String s = type.getString(type.decompose(value));

        if (type instanceof UTF8Type)
            return String.format("'%s'", s.replaceAll("'", "''"));

        if (type instanceof BytesType)
            return "0x" + s;

        return s;
    }

    private static ByteBuffer makeByteBuffer(Object value, AbstractType type)
    {
        if (value == null)
            return null;

        if (value instanceof TupleValue)
            return ((TupleValue)value).toByteBuffer();

        if (value instanceof ByteBuffer)
            return (ByteBuffer)value;

        return type.decompose(value);
    }

    private static String formatValue(ByteBuffer bb, AbstractType<?> type)
    {
        return bb == null ? "null" : type.getString(bb);
    }

    protected Object tuple(Object...values)
    {
        return new TupleValue(values);
    }

    protected Object list(Object...values)
    {
        return Arrays.asList(values);
    }

    protected Object set(Object...values)
    {
        return ImmutableSet.copyOf(values);
    }

    protected Object map(Object...values)
    {
        if (values.length % 2 != 0)
            throw new IllegalArgumentException();

        int size = values.length / 2;
        Map m = new HashMap(size);
        for (int i = 0; i < size; i++)
            m.put(values[2 * i], values[(2 * i) + 1]);
        return m;
    }

    // Attempt to find an AbstracType from a value (for serialization/printing sake).
    // Will work as long as we use types we know of, which is good enough for testing
    private static AbstractType typeFor(Object value)
    {
        if (value instanceof ByteBuffer || value instanceof TupleValue || value == null)
            return BytesType.instance;

        if (value instanceof Integer)
            return Int32Type.instance;

        if (value instanceof Long)
            return LongType.instance;

        if (value instanceof Float)
            return FloatType.instance;

        if (value instanceof Double)
            return DoubleType.instance;

        if (value instanceof String)
            return UTF8Type.instance;

        if (value instanceof List)
        {
            List l = (List)value;
            AbstractType elt = l.isEmpty() ? BytesType.instance : typeFor(l.get(0));
            return ListType.getInstance(elt);
        }

        if (value instanceof Set)
        {
            Set s = (Set)value;
            AbstractType elt = s.isEmpty() ? BytesType.instance : typeFor(s.iterator().next());
            return SetType.getInstance(elt);
        }

        if (value instanceof Map)
        {
            Map m = (Map)value;
            AbstractType keys, values;
            if (m.isEmpty())
            {
                keys = BytesType.instance;
                values = BytesType.instance;
            }
            else
            {
                Map.Entry entry = (Map.Entry)m.entrySet().iterator().next();
                keys = typeFor(entry.getKey());
                values = typeFor(entry.getValue());
            }
            return MapType.getInstance(keys, values);
        }

        throw new IllegalArgumentException("Unsupported value type (value is " + value + ")");
    }

    private static class TupleValue
    {
        private final Object[] values;

        TupleValue(Object[] values)
        {
            this.values = values;
        }

        public ByteBuffer toByteBuffer()
        {
            ByteBuffer[] bbs = new ByteBuffer[values.length];
            for (int i = 0; i < values.length; i++)
                bbs[i] = makeByteBuffer(values[i], typeFor(values[i]));
            return TupleType.buildValue(bbs);
        }

        public String toCQLString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < values.length; i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(formatForCQL(values[i]));
            }
            sb.append(")");
            return sb.toString();
        }
    }
}
