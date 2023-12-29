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
package org.apache.cassandra.db.monitoring;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.metrics.BadQueryMetrics;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaTransformations;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Report BadQueries in table.
 */
public class BadQueriesInTable extends BadQueryReporter
{
    private static final Logger logger = LoggerFactory.getLogger(BadQueriesInTable.class);

    private static final String TABLE_NAME = "badquery";
    private static final String TABLE_SCHEMA =
            "CREATE TABLE " + TABLE_NAME + "("
                    + "policyid int,"
                    + "sampleid int,"
                    + "ksname text,"
                    + "tablename text,"
                    + "key text,"
                    + "details text,"
                    + "PRIMARY KEY (policyid, sampleid))";
    private final static String INSERT_QUERY = String.format(
    "INSERT INTO %s.%s (policyid, sampleid, ksname, tablename, key, details) values (?, ?, ?, ?, ?, ?)"
            , SchemaConstants.BAD_QUERY_MONITOR_KEYSPACE_NAME, TABLE_NAME);

    private static ModificationStatement modificationStatement;

    private KeyspaceMetadata getKeyspaceMetadata()
    {
        TableMetadata limits = CreateTableStatement.parse(TABLE_SCHEMA, SchemaConstants.BAD_QUERY_MONITOR_KEYSPACE_NAME)
                                                   .id(TableId.forSystemTable(SchemaConstants.BAD_QUERY_MONITOR_KEYSPACE_NAME, TABLE_NAME))
                                                   .comment("badquery details")
                                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90)).build();
        return KeyspaceMetadata.create(SchemaConstants.BAD_QUERY_MONITOR_KEYSPACE_NAME, KeyspaceParams.simple(1), Tables.of(limits));
    }

    static
    {
        for (BadQuery.BadQueryCategory category : BadQuery.BadQueryCategory.values())
        {
            BAD_QUERY_CATEGORY_QUEUES.put(category, new ConcurrentLinkedQueue<BadQueryTypes>());
            CURRENT_SAMPLES.put(category, new AtomicInteger(0));
        }
    }

    @Override
    public void setup()
    {
        KeyspaceMetadata ksm = getKeyspaceMetadata();
        try
        {
            Schema.instance.transform(SchemaTransformations.updateSystemKeyspace(ksm, 0));
        }
        catch (AlreadyExistsException e)
        {
            logger.debug("Attempted to create new keyspace {}, but it already exists", ksm.name);
        }

        modificationStatement = (ModificationStatement) QueryProcessor.getStatement(INSERT_QUERY, ClientState.forInternalCalls());

        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> log(),
                5,
                DatabaseDescriptor.getBadQueryLoggingInterval(),
                TimeUnit.SECONDS);
        BadQueryMetrics.setup();
    }

    @Override
    public void reportBadQuery(BadQuery.BadQueryCategory queryType, BadQueryTypes operationDetails)
    {
        /*TODO: ideally we should insert each of this events directly in table which will make it more real time
        but we don't know performance penalty of INSERTS even though we keep table very very small.
        Analyze and see if we can make ModificationStatement.execute almost 0ms*/
        if (CURRENT_SAMPLES.get(queryType).get() < DatabaseDescriptor.getBadQueryMaxSamplesPerIntervalInSyslog())
        {
            BAD_QUERY_CATEGORY_QUEUES.get(queryType).offer(operationDetails);
            CURRENT_SAMPLES.get(queryType).incrementAndGet();

            // emit metric for large partition reads (table level)
            if (operationDetails instanceof LargePartition) {
                LargePartition op = (LargePartition) operationDetails;
                ColumnFamilyStore cfs = Keyspace.open(operationDetails.keySpace)
                                                .getColumnFamilyStore(operationDetails.tableName);
                if (queryType == BadQuery.BadQueryCategory.LARGE_PARTITION_READ) {
                    cfs.metric.largePartitionReadSize.inc(op.getSize());
                }
                if (queryType == BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE) {
                    cfs.metric.largePartitionWriteSize.inc(op.getSize());
                }
            }
        }
    }

    /**
     * log bad query using BadQuery reporter.
     */
    private static void log()
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(SchemaConstants.BAD_QUERY_MONITOR_KEYSPACE_NAME);
        boolean dcawareDeployment = ks.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class;
        for (BadQuery.BadQueryCategory type : BadQuery.BadQueryCategory.values())
        {
            ConcurrentLinkedQueue<BadQueryTypes> badQueries = BAD_QUERY_CATEGORY_QUEUES.get(type);
            boolean cleanupDone = false;
            int sampleId = 0;
            while (!badQueries.isEmpty())
            {
                BadQueryTypes bq = badQueries.poll();
                if (!cleanupDone)
                {
                    bq.cleanup();
                    cleanupDone = true;
                }
                modificationStatement.execute(QueryState.forInternalCalls(),
                        QueryOptions.forInternalCalls(dcawareDeployment ? ConsistencyLevel.LOCAL_ONE : ConsistencyLevel.ONE,
                                Lists.newArrayList(ByteBufferUtil.bytes(type.ordinal()),
                                        ByteBufferUtil.bytes(sampleId++),
                                        ByteBufferUtil.bytes(bq.keySpace),
                                        ByteBufferUtil.bytes(bq.tableName),
                                        ByteBufferUtil.bytes(bq.getKey()),
                                        ByteBufferUtil.bytes(bq.getDetails()))),
                                              Dispatcher.RequestTime.forImmediateExecution());
            }
            //reset sample count so we get next batch of bad queries
            CURRENT_SAMPLES.get(type).set(0);
        }
    }
}
