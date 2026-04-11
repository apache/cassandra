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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;

import static com.datastax.driver.core.ParseUtils.doubleQuote;
import static org.assertj.core.api.Assertions.assertThat;

public class SystemKeyspacesDataLocationTest extends TestBaseImpl
{
    @Test
    public void preparedStatementsShouldNotBeMovedOnStartupTest() throws IOException, ExecutionException, InterruptedException
    {
        try (Cluster cluster = Cluster.build(1).withDataDirCount(3).start())
        {
            // to test the behaviour we need have sstables of prepared statements in more than one data directory
            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore preparedStatementsCFS = ColumnFamilyStore.getIfExists(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS);
                preparedStatementsCFS.disableAutoCompaction();
                createSSTablesForPreparedStatementsTable(preparedStatementsCFS);
            });

            Set<String> preparedStatementsDataLocationsBefore = getPreparedStatementsDataLocations(cluster.get(1));
            assertThat(preparedStatementsDataLocationsBefore).hasSizeGreaterThan(1);
            cluster.get(1).shutdown().get();

            // we expect that for prepared statements, sstables will not be moved to the first data directory on startup
            cluster.get(1).startup();
            Set<String> preparedStatementsDataLocationsAfter = getPreparedStatementsDataLocations(cluster.get(1));
            assertThat(preparedStatementsDataLocationsAfter).isEqualTo(preparedStatementsDataLocationsBefore);
        }
    }

    private static Set<String> getPreparedStatementsDataLocations(IInvokableInstance i)
    {
        return i.callOnInstance(() -> ColumnFamilyStore.getIfExists(SchemaConstants.SYSTEM_KEYSPACE_NAME, SystemKeyspace.PREPARED_STATEMENTS)
                                                       .getLiveSSTables().stream()
                                                       // sstables are located at <data-location>/<keyspace-name>/<table-name-with-id> so we need to go up two directories to get the data location
                                                       .map(sstr -> sstr.descriptor.directory.parent().parent().toString())
                                                       .collect(Collectors.toSet()));
    }

    private static void createSSTablesForPreparedStatementsTable(ColumnFamilyStore preparedStatementsCFS)
    {
        for (String keyspaceName : SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES)
        {
            for (TableMetadata tableMetadata : Schema.instance.getKeyspaceMetadata(keyspaceName).tables)
            {
                String query = String.format("select * from %s.%s", keyspaceName, doubleQuote(tableMetadata.name));
                QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(query);
                QueryProcessor.storePreparedStatement(query, keyspaceName, prepared);
                preparedStatementsCFS.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
            }
        }
    }
}
