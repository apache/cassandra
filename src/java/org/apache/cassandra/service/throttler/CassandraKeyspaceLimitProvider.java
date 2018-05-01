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

package org.apache.cassandra.service.throttler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * CassandraKeyspaceLimitProvider uses a Cassandra table to store the request throttler limits.
 * <p>
 * CREATE KEYSPACE system_throttle WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
 * <p>
 * CREATE TABLE system_throttle.limits (
 *   partition int,
 *   keyspace_name text,
 *   range_read_limit int,
 *   serial_mutation_limit int,
 *   serial_read_limit int,
 *   single_mutation_limit int,
 *   single_read_limit int,
 *   PRIMARY KEY (partition, keyspace_name)
 * )
 * <p>
 * The expected contents are:
 * <p>
 * partition | keyspace_name | range_read_limit | serial_mutation_limit | serial_read_limit | single_mutation_limit | single_read_limit
 * -----------+---------------+------------------+-----------------------+-------------------+-----------------------+-------------------
 * 0 |           tmp |               10 |                     1 |                 1 |                     1 |                 1
 * 0 |          tmp2 |                1 |                     2 |                 3 |                     4 |                 5
 * 0 | default-per-keyspace-limit |  20 |                    20 |                20 |                    20 |                20
 * <p>
 *
 * The default-per-keyspace-limit is applied to every non-system keyspace even if it does not exist in this table.
 *
 * We use a single partition with value 0 so that it is easy and efficient to get the throttle limits for all the keyspaces
 * in a single partition query.
 **/
public class CassandraKeyspaceLimitProvider
{
    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 0: original definition in 3.0
     */
    public static final long GENERATION = 0;

    private static final Logger logger = LoggerFactory.getLogger(CassandraKeyspaceLimitProvider.class);

    private static final String keyspaceName = "system_throttle";
    private static final String tableName = "limits";
    private static final int singlePartitonKey = 0;
    private static final String tableSchema =
        "CREATE TABLE " + tableName + "("
        + "partition int,"
        + "keyspace_name text,"
        + "single_read_limit int,"
        + "serial_read_limit int,"
        + "range_read_limit int,"
        + "single_mutation_limit int,"
        + "serial_mutation_limit int,"
        + "PRIMARY KEY ((partition), keyspace_name))";

    private final static String selectQuery = String.format(
        "SELECT partition,keyspace_name,single_read_limit,serial_read_limit,range_read_limit,single_mutation_limit,serial_mutation_limit"
        + " FROM %s.%s WHERE partition=%d", keyspaceName, tableName, singlePartitonKey);

    private SelectStatement selectStatement;

    public void setup()
    {
        selectStatement = (SelectStatement) QueryProcessor.getStatement(selectQuery, ClientState.forInternalCalls());
    }

    public KeyspaceMetadata getKeyspaceMetadata()
    {
        TableMetadata limits = CreateTableStatement.parse(tableSchema, keyspaceName)
                                                   .comment("Throttling limits on different keyspaces")
                                                   .gcGraceSeconds((int) TimeUnit.DAYS.toSeconds(90)).build();;

        return KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(1), Tables.of(limits));
    }

    public Map<String, KeyspaceLimits> getKeyspaceLimits() throws RequestExecutionException, RequestValidationException
    {
        ResultMessage.Rows rows = selectStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE, null), Dispatcher.RequestTime.forImmediateExecution());
        UntypedResultSet result = UntypedResultSet.create(rows.result);
        Map<String, KeyspaceLimits> map = new ConcurrentHashMap<>();
        for (UntypedResultSet.Row row : result)
        {
            KeyspaceLimits limits = new KeyspaceLimits();
            limits.singleReadLimit.set(row.getInt("single_read_limit"));
            limits.serialReadLimit.set(row.getInt("serial_read_limit"));
            limits.rangeReadLimit.set(row.getInt("range_read_limit"));
            limits.singleMutationLimit.set(row.getInt("single_mutation_limit"));
            limits.serialMutationLimit.set(row.getInt("serial_mutation_limit"));
            map.put(row.getString("keyspace_name"), limits);
        }
        return map;
    }
}
