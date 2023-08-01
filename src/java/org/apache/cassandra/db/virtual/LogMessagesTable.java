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

import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Virtual table for holding Cassandra logs. Entries to this table are added via log appender.
 * <p>
 * The virtual table is bounded in its size. If a new log message is appended to virtual table,
 * the oldest one is removed.
 * <p>
 * This virtual table can be truncated.
 * <p>
 * This table does not enable {@code ALLOW FILTERING} implicitly.
 *
 * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-18238">CASSANDRA-18238</a>
 * @see org.apache.cassandra.utils.logging.VirtualTableAppender
 */
public final class LogMessagesTable extends AbstractMutableVirtualTable
{
    private static final Logger logger = LoggerFactory.getLogger(LogMessagesTable.class);

    public static final int LOGS_VIRTUAL_TABLE_MIN_ROWS = 1000;
    public static final int LOGS_VIRTUAL_TABLE_DEFAULT_ROWS = 50_000;
    public static final int LOGS_VIRTUAL_TABLE_MAX_ROWS = 100_000;

    public static final String TABLE_NAME = "system_logs";
    private static final String TABLE_COMMENT = "Cassandra logs";

    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String LOGGER_COLUMN_NAME = "logger";
    public static final String ORDER_IN_MILLISECOND_COLUMN_NAME = "order_in_millisecond";
    public static final String LEVEL_COLUMN_NAME = "level";
    public static final String MESSAGE_COLUMN_NAME = "message";

    private final List<LogMessage> buffer;

    LogMessagesTable(String keyspace)
    {
        this(keyspace, resolveBufferSize());
    }

    @VisibleForTesting
    LogMessagesTable(String keyspace, int size)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_COMMENT)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(TimestampType.instance))
                           .addPartitionKeyColumn(TIMESTAMP_COLUMN_NAME, TimestampType.instance)
                           .addClusteringColumn(ORDER_IN_MILLISECOND_COLUMN_NAME, Int32Type.instance)
                           .addRegularColumn(LOGGER_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(LEVEL_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(MESSAGE_COLUMN_NAME, UTF8Type.instance).build());

        logger.debug("capacity of virtual table {} is set to be at most {} rows", metadata().toString(), size);
        buffer = BoundedLinkedList.create(size);
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata(), DecoratedKey.comparator.reversed());

        synchronized (buffer)
        {
            long milliSecondsOfPreviousLog = 0;
            long milliSecondsOfCurrentLog;

            int index = 0;

            Iterator<LogMessage> iterator = buffer.listIterator();
            while (iterator.hasNext())
            {
                LogMessage log = iterator.next();

                milliSecondsOfCurrentLog = log.timestamp;
                if (milliSecondsOfPreviousLog == milliSecondsOfCurrentLog)
                    ++index;
                else
                    index = 0;

                milliSecondsOfPreviousLog = milliSecondsOfCurrentLog;

                result.row(new Date(log.timestamp), index)
                      .column(LOGGER_COLUMN_NAME, log.logger)
                      .column(LEVEL_COLUMN_NAME, log.level)
                      .column(MESSAGE_COLUMN_NAME, log.message);
            }
        }

        return result;
    }

    public void add(LoggingEvent event)
    {
        buffer.add(new LogMessage(event));
    }

    @Override
    public void truncate()
    {
        buffer.clear();
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return false;
    }

    @VisibleForTesting
    static int resolveBufferSize()
    {
        int size = CassandraRelevantProperties.LOGS_VIRTUAL_TABLE_MAX_ROWS.getInt();
        return (size < LOGS_VIRTUAL_TABLE_MIN_ROWS || size > LOGS_VIRTUAL_TABLE_MAX_ROWS)
               ? LOGS_VIRTUAL_TABLE_DEFAULT_ROWS : size;
    }

    @VisibleForTesting
    public static class LogMessage
    {
        public final long timestamp;
        public final String logger;
        public final String level;
        public final String message;

        public LogMessage(LoggingEvent event)
        {
            this(event.getTimeStamp(), event.getLoggerName(), event.getLevel().toString(), event.getFormattedMessage());
        }

        public LogMessage(long timestamp, String logger, String level, String message)
        {
            this.timestamp = timestamp;
            this.logger = logger;
            this.level = level;
            this.message = message;
        }
    }

    private static final class BoundedLinkedList<T> extends LinkedList<T>
    {
        private final int maxSize;

        public static <T> List<T> create(int size)
        {
            return Collections.synchronizedList(new BoundedLinkedList<>(size));
        }

        private BoundedLinkedList(int maxSize)
        {
            this.maxSize = maxSize;
        }

        @Override
        public boolean add(T t)
        {
            if (size() == maxSize)
                removeLast();

            addFirst(t);

            return true;
        }
    }
}
