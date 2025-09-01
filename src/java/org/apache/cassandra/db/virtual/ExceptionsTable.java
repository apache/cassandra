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
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

public class ExceptionsTable extends AbstractCapacityBoundVirtualTable<ExceptionsTable.ExceptionRow>
{
    /**
     * Buffer of uncaught exceptions which happened while virtual table was not initialized.
     */
    public static final List<ExceptionRow> preInitialisationBuffer = new ArrayList<>();

    public static final String EXCEPTIONS_TABLE_NAME = "uncaught_exceptions";
    private static final String EXCEPTION_CLASS_COLUMN_NAME = "exception_class";
    private static final String COUNT_COLUMN_NAME = "count";
    private static final String LAST_MESSAGE_COLUMN_NAME = "last_message";
    private static final String LAST_STACKTRACE_COLUMN_NAME = "last_stacktrace";
    private static final String LAST_OCCURENCE_COLUMN_NAME = "last_occurence";

    // please be sure operations on this structure are thread-safe
    protected final List<ExceptionRow> buffer;

    ExceptionsTable(String keyspace)
    {
        this(keyspace, 1000);
    }

    ExceptionsTable(String keyspace, int maxSize)
    {
        super(TableMetadata.builder(keyspace, EXCEPTIONS_TABLE_NAME)
                           .comment("View into uncaught exceptions")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(EXCEPTION_CLASS_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(COUNT_COLUMN_NAME, Int32Type.instance)
                           .addRegularColumn(LAST_MESSAGE_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(LAST_STACKTRACE_COLUMN_NAME, ListType.getInstance(UTF8Type.instance, false))
                           .addRegularColumn(LAST_OCCURENCE_COLUMN_NAME, TimestampType.instance)
                           .build());

        this.buffer = BoundedLinkedList.create(maxSize);
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return true;
    }

    /**
     * @param exceptionClass exception class of uncaught exception
     * @param message        message of given exception
     * @param stackTrace     whole stacktrace of given exception
     * @param occurenceTime  time when given exception ocurred
     */
    public synchronized void add(String exceptionClass,
                                 String message,
                                 StackTraceElement[] stackTrace,
                                 long occurenceTime)
    {
        ExceptionRow mergeInto = null;
        for (ExceptionRow row : buffer)
        {
            if (row.exceptionClass.equals(exceptionClass))
            {
                mergeInto = row;
                break;
            }
        }

        if (mergeInto == null)
        {
            buffer.add(new ExceptionRow(exceptionClass, message, stackTrace, occurenceTime));
        }
        else
        {
            mergeInto.count += 1;
            mergeInto.message = message;
            mergeInto.stackTrace = ExceptionRow.extractStacktrace(new ArrayList<>(), stackTrace);
            mergeInto.occurence = new Date(occurenceTime);
        }
    }

    public synchronized void flush()
    {
        for (ExceptionRow row : preInitialisationBuffer)
            add(row.exceptionClass, row.message, row.stackTrace.toArray(new StackTraceElement[0]), row.occurence.getTime());

        preInitialisationBuffer.clear();
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        synchronized (buffer)
        {
            Iterator<ExceptionRow> iterator = buffer.iterator();
            while (iterator.hasNext())
            {
                ExceptionRow exceptionRow = iterator.next();

                result.row(exceptionRow.exceptionClass)
                      .column(COUNT_COLUMN_NAME, exceptionRow.count)
                      .column(LAST_MESSAGE_COLUMN_NAME, exceptionRow.message)
                      .column(LAST_STACKTRACE_COLUMN_NAME, exceptionRow.stackTrace)
                      .column(LAST_OCCURENCE_COLUMN_NAME, exceptionRow.occurence);
            }
        }

        return result;
    }

    @Override
    public void truncate()
    {
        synchronized (buffer)
        {
            buffer.clear();
        }
    }

    public static class ExceptionRow
    {
        public String exceptionClass;
        public int count;
        public String message;
        public List<String> stackTrace = new ArrayList<>();
        public Date occurence;

        /**
         * @param exceptionClass  exception class of uncaught exception
         * @param message         message of given exception
         * @param stackTraceArray whole stacktrace of given exception
         * @param count           number of times given exception has occurred
         * @param occurenceTime   time when given exception ocurred, in milliseconds from epoch
         */
        public ExceptionRow(String exceptionClass,
                            String message,
                            StackTraceElement[] stackTraceArray,
                            int count,
                            long occurenceTime)
        {
            this.exceptionClass = exceptionClass;
            this.count = count;
            this.message = message;
            this.occurence = new Date(occurenceTime);

            extractStacktrace(stackTrace, stackTraceArray);
        }

        /**
         * @param exceptionClass  exception class of uncaught exception
         * @param message         message of given exception
         * @param stackTraceArray whole stacktrace of given exception
         * @param occurenceTime   time when given exception ocurred, in milliseconds from epoch
         */
        public ExceptionRow(String exceptionClass,
                            String message,
                            StackTraceElement[] stackTraceArray,
                            long occurenceTime)
        {
            this(exceptionClass, message, stackTraceArray, 1, occurenceTime);
        }

        public static List<String> extractStacktrace(List<String> result, StackTraceElement[] stackTraceArray)
        {
            for (StackTraceElement element : stackTraceArray)
                result.add(element.toString());

            return result;
        }
    }
}
