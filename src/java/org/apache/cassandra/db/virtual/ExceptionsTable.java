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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.logging.AbstractVirtualTableAppender;

public class ExceptionsTable extends AbstractMutableVirtualTable
{
    public static final String EXCEPTIONS_TABLE_NAME = "uncaught_exceptions";
    public static final String EXCEPTION_CLASS_COLUMN_NAME = "exception_class";
    public static final String EXCEPTION_LOCATION_COLUMN_NAME = "exception_location";
    public static final String COUNT_COLUMN_NAME = "count";
    public static final String LAST_MESSAGE_COLUMN_NAME = "last_message";
    public static final String LAST_STACKTRACE_COLUMN_NAME = "last_stacktrace";
    public static final String LAST_OCCURRENCE_COLUMN_NAME = "last_occurrence";

    /**
     * Buffer of uncaught exceptions which happened while virtual table was not initialized.
     */
    static final List<ExceptionRow> preInitialisationBuffer = Collections.synchronizedList(new ArrayList<>());

    @VisibleForTesting
    static volatile ExceptionsTable INSTANCE;

    // please be sure operations on this structure are thread-safe
    @VisibleForTesting
    final BoundedMap buffer;

    ExceptionsTable(String keyspace)
    {
        // for starters capped to 1k, I do not think we need to make this configurable (yet).
        this(keyspace, 1000);
    }

    ExceptionsTable(String keyspace, int maxSize)
    {
        super(TableMetadata.builder(keyspace, EXCEPTIONS_TABLE_NAME)
                           .comment("View into uncaught exceptions")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(EXCEPTION_CLASS_COLUMN_NAME, UTF8Type.instance)
                           .addClusteringColumn(EXCEPTION_LOCATION_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(COUNT_COLUMN_NAME, Int32Type.instance)
                           .addRegularColumn(LAST_MESSAGE_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(LAST_STACKTRACE_COLUMN_NAME, ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn(LAST_OCCURRENCE_COLUMN_NAME, TimestampType.instance)
                           .build());

        this.buffer = new BoundedMap(maxSize);
    }

    public void flush()
    {
        for (ExceptionRow row : preInitialisationBuffer)
            add(row.exceptionClass, row.exceptionLocation, row.message, row.stackTrace, row.occurrence.getTime());

        preInitialisationBuffer.clear();
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        synchronized (buffer)
        {
            for (Map.Entry<String, LinkedHashMap<String, ExceptionRow>> partition : buffer.entrySet())
            {
                for (Map.Entry<String, ExceptionRow> entry : partition.getValue().entrySet())
                    populateRow(result, partition.getKey(), entry.getKey(), entry.getValue());
            }
        }

        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        synchronized (buffer)
        {
            String exceptionClass = UTF8Type.instance.getSerializer().deserialize(partitionKey.getKey());
            LinkedHashMap<String, ExceptionRow> partition = buffer.get(exceptionClass);

            if (partition != null)
            {
                for (Map.Entry<String, ExceptionRow> row : partition.entrySet())
                    populateRow(result, exceptionClass, row.getKey(), row.getValue());
            }
        }

        return result;
    }

    private void populateRow(SimpleDataSet result, String exceptionClass, String exceptionLocation, ExceptionRow row)
    {
        result.row(exceptionClass, exceptionLocation)
              .column(COUNT_COLUMN_NAME, row.count)
              .column(LAST_MESSAGE_COLUMN_NAME, row.message)
              .column(LAST_STACKTRACE_COLUMN_NAME, row.stackTrace)
              .column(LAST_OCCURRENCE_COLUMN_NAME, row.occurrence);
    }

    @Override
    public void truncate()
    {
        synchronized (buffer)
        {
            buffer.clear();
        }
    }

    static List<String> extractStacktrace(StackTraceElement[] stackTraceArray)
    {
        List<String> result = new ArrayList<>(stackTraceArray.length);

        for (StackTraceElement element : stackTraceArray)
            result.add(element.toString());

        return result;
    }

    public static void persist(Throwable t)
    {
        if (INSTANCE == null)
            INSTANCE = AbstractVirtualTableAppender.getVirtualTable(ExceptionsTable.class, EXCEPTIONS_TABLE_NAME);

        Throwable toPersist = t;

        while (toPersist.getCause() != null)
            toPersist = toPersist.getCause();

        List<String> stackTrace = extractStacktrace(toPersist.getStackTrace());
        long now = Clock.Global.currentTimeMillis();

        if (INSTANCE != null)
        {
            INSTANCE.add(toPersist.getClass().getName(),
                         stackTrace.isEmpty() ? "unknown" : stackTrace.get(0),
                         toPersist.getMessage(),
                         stackTrace,
                         now);
        }
        else
        {
            preInitialisationBuffer.add(new ExceptionRow(toPersist.getClass().getName(),
                                                         stackTrace.isEmpty() ? "unknown" : stackTrace.get(0),
                                                         0,
                                                         toPersist.getMessage(),
                                                         stackTrace,
                                                         now));
        }
    }

    /**
     * Adds entry to internal buffer.
     *
     * @param exceptionClass    exception class of uncaught exception
     * @param exceptionLocation location where that exception was thrown
     * @param message           message of given exception
     * @param stackTrace        whole stacktrace of given exception
     * @param occurrenceTime    time when given exception ocurred
     */
    private void add(String exceptionClass,
                     String exceptionLocation,
                     String message,
                     List<String> stackTrace,
                     long occurrenceTime)
    {
        synchronized (buffer)
        {
            Map<String, ExceptionRow> exceptionRowWithLocation = buffer.computeIfAbsent(exceptionClass, (classToAdd) -> new LinkedHashMap<>());
            ExceptionRow exceptionRow = exceptionRowWithLocation.get(exceptionLocation);
            if (exceptionRow == null)
            {
                // exception class and location can be null for value as we have it as part of keys already
                exceptionRow = new ExceptionRow(null, null, 1, message, stackTrace, occurrenceTime);
                exceptionRowWithLocation.put(exceptionLocation, exceptionRow);
                // not important, can be null
                // we need to do this, because if we add into a map which is
                // a value of some buffer key, we might exceed the number
                // of overall entries in the buffer
                buffer.removeEldestEntry(null);
            }
            else
            {
                exceptionRow.count += 1;
                exceptionRow.message = message;
                exceptionRow.stackTrace = stackTrace;
                exceptionRow.occurrence = new Date(occurrenceTime);
            }
        }
    }

    static final class ExceptionRow
    {
        final String exceptionClass;
        final String exceptionLocation;
        int count;
        String message;
        List<String> stackTrace;
        Date occurrence;

        /**
         * @param exceptionClass    exception class of uncaught exception
         * @param exceptionLocation location where that exception was thrown
         * @param message           message of given exception
         * @param stackTrace        whole stacktrace of given exception
         * @param occurrenceTime    time when given exception ocurred, in milliseconds from epoch
         */
        ExceptionRow(String exceptionClass,
                     String exceptionLocation,
                     int count,
                     String message,
                     List<String> stackTrace,
                     long occurrenceTime)
        {
            this.exceptionClass = exceptionClass;
            this.exceptionLocation = exceptionLocation;
            this.count = count;
            this.stackTrace = stackTrace;
            this.message = message;
            this.occurrence = new Date(occurrenceTime);
        }
    }

    @VisibleForTesting
    static class BoundedMap extends LinkedHashMap<String, LinkedHashMap<String, ExceptionRow>>
    {
        private final int maxSize;

        public BoundedMap(int maxSize)
        {
            if (maxSize <= 0)
                throw new IllegalArgumentException("maxSize has to be bigger than 0");

            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, LinkedHashMap<String, ExceptionRow>> eldest)
        {
            if (computeSize() > maxSize)
            {
                String oldestExceptionClass = null;
                String oldestExceptionLocation = null;
                long oldestLastOccurrence = Long.MAX_VALUE;
                for (Map.Entry<String, LinkedHashMap<String, ExceptionRow>> entry : entrySet())
                {
                    for (Map.Entry<String, ExceptionRow> entryInEntry : entry.getValue().entrySet())
                    {
                        long currentLastOccurrence = entryInEntry.getValue().occurrence.getTime();
                        if (currentLastOccurrence < oldestLastOccurrence)
                        {
                            oldestExceptionLocation = entryInEntry.getKey();
                            oldestExceptionClass = entry.getKey();
                            oldestLastOccurrence = currentLastOccurrence;
                        }
                    }
                }

                if (oldestLastOccurrence < Long.MAX_VALUE)
                {
                    LinkedHashMap<String, ExceptionRow> aMap = get(oldestExceptionClass);
                    if (aMap.size() == 1)
                        remove(oldestExceptionClass);
                    else
                        aMap.remove(oldestExceptionLocation);
                }
            }

            // always returning false as per method's contract saying that
            // overrides might modify the map directly but in that case it must return false
            return false;
        }

        private int computeSize()
        {
            int size = 0;

            for (LinkedHashMap<String, ExceptionRow> value : values())
                size += value.size();

            return size;
        }
    }
}
