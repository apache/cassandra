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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.LoggingEvent;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.monitoring.MonitoringTask.Operation;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SlowQueriesTable extends AbstractLoggerVirtualTable<Operation>
{
    private static final Logger logger = LoggerFactory.getLogger(SlowQueriesTable.class);

    public static final int LOGS_VIRTUAL_TABLE_DEFAULT_ROWS = 10_000;
    public static final int LOGS_VIRTUAL_TABLE_MAX_ROWS = 100_000;

    public static final String TABLE_NAME = "slow_queries";
    private static final String TABLE_COMMENT = "Slow queries";

    public static final String KEYSPACE_COLUMN_NAME = "keyspace_name";
    public static final String TABLE_COLUMN_NAME = "table_name";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";
    public static final String QUERY_COLUMN_NAME = "query";
    public static final String MINIMUM_TIME_COLUMN_NAME = "min_ms";
    public static final String MAXIMUM_TIME_COLUMN_NAME = "max_ms";
    public static final String AVERAGE_TIME_COLUMN_NAME = "avg_ms";
    public static final String TIMES_REPORTED_COLUMN_NAME = "times_reported";
    public static final String CROSS_NODE_COLUMN_NAME = "cross_node";

    SlowQueriesTable(String keyspace)
    {
        this(keyspace, resolveBufferSize(CassandraRelevantProperties.LOGS_SLOW_QUERIES_VIRTUAL_TABLE_MAX_ROWS.getInt(),
                                         LOGS_VIRTUAL_TABLE_MAX_ROWS,
                                         LOGS_VIRTUAL_TABLE_DEFAULT_ROWS));
    }

    @VisibleForTesting
    SlowQueriesTable(String keyspace, int size)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_COMMENT)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_COLUMN_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_COLUMN_NAME, UTF8Type.instance)
                           .addClusteringColumn(TIMESTAMP_COLUMN_NAME, TimestampType.instance)
                           // We are adding query as a clustering column for uniqueness,
                           // In theory, it might happen that two monitoring operations
                           // would be emitted for same keyspace, same table at the exact same time
                           // (in milliseconds). That means that one operation would "shadow"
                           // another one because primary key would be same for both.
                           // To make it truly unique, we include query among clustering keys
                           // as well. If queries were same, then they would be also reported so
                           // (it would be reflected in "times_reported" column)
                           .addClusteringColumn(QUERY_COLUMN_NAME, UTF8Type.instance)
                           .addRegularColumn(MINIMUM_TIME_COLUMN_NAME, LongType.instance)
                           .addRegularColumn(MAXIMUM_TIME_COLUMN_NAME, LongType.instance)
                           .addRegularColumn(AVERAGE_TIME_COLUMN_NAME, LongType.instance)
                           .addRegularColumn(TIMES_REPORTED_COLUMN_NAME, Int32Type.instance)
                           .addRegularColumn(CROSS_NODE_COLUMN_NAME, BooleanType.instance)
                           .build(),
              size);
    }

    @Override
    protected void applyPartitionDeletion(ColumnValues partitionKey)
    {
        String keyspace = partitionKey.value(0);

        synchronized (buffer)
        {
            buffer.removeIf(o -> o.keyspace().equals(keyspace));
        }
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata(), DecoratedKey.comparator.reversed());

        synchronized (buffer)
        {
            Iterator<Operation> iterator = buffer.listIterator();
            while (iterator.hasNext())
            {
                Operation operation = iterator.next();

                result.row(operation.keyspace(), operation.table(), new Date(operation.timestampMs()), operation.name())
                      .column(MINIMUM_TIME_COLUMN_NAME, NANOSECONDS.toMillis(operation.minTimeNanos()))
                      .column(MAXIMUM_TIME_COLUMN_NAME, NANOSECONDS.toMillis(operation.maxTimeNanos()))
                      .column(AVERAGE_TIME_COLUMN_NAME, NANOSECONDS.toMillis(operation.averageTime()))
                      .column(TIMES_REPORTED_COLUMN_NAME, operation.numTimesReported())
                      .column(CROSS_NODE_COLUMN_NAME, operation.isCrossNode());
            }
        }

        return result;
    }

    @Override
    public List<Operation> getMessages(LoggingEvent event)
    {
        try
        {
            List<Operation> qualified = new ArrayList<>();
            for (Operation operation : Operation.deserialize(event.getMessage()))
            {

                // in (improbable) case there is an operation which does not have
                // keyspace / table on it, we just skip this from processing
                // as we would have nothing to show for partition key and clustering column
                if (operation.keyspace() == null || operation.table() == null)
                    continue;

                // if cf of an operation is present, take keyspace and table name from it
                // instead of having new string instances per operation which might
                // take relatively a lot of additional space unnecessarily
                Keyspace keyspace = Keyspace.openIfExists(operation.keyspace());
                String keyspaceName;
                String tableName;
                if (keyspace != null)
                {
                    keyspaceName = keyspace.getName();
                    try
                    {
                        ColumnFamilyStore table = keyspace.getColumnFamilyStore(operation.table());
                        tableName = table.getTableName();
                    }
                    catch (IllegalArgumentException ex)
                    {
                        tableName = operation.table();
                    }
                }
                else
                {
                    keyspaceName = operation.keyspace();
                    tableName = operation.table();
                }

                operation.setKeyspace(keyspaceName);
                operation.setTable(tableName);
                qualified.add(operation);
            }

            return qualified;
        }
        catch (Throwable t)
        {
            logger.trace("Unable to generate list of slow queries", t);
            return null;
        }
    }

    @Override
    public boolean allowFilteringImplicitly()
    {
        return true;
    }
}
