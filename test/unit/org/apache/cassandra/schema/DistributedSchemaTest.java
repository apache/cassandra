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

package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.service.reads.PercentileSpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.assertj.core.api.Assertions.assertThat;

public class DistributedSchemaTest
{

    @BeforeClass
    public static void beforeClass()
    {
        ServerTestUtils.prepareServerNoRegister();
    }

    @Test
    public void fromSystemTablesPreservesExistingSchemaProperties()
    {
        TableMetadata defaultTable  = AuthKeyspace.metadata().tables.getNullable(AuthKeyspace.ROLES);
        TableMetadata modifiedTable = defaultTable.unbuild()
                                                  .id(TableId.fromLong(0))
                                                  .comment("Testing")
                                                  .gcGraceSeconds(60)
                                                  .compaction(CompactionParams.lcs(new HashMap<>()))
                                                  .speculativeRetry(new PercentileSpeculativeRetryPolicy(50.0))
                                                  .build();
        assertThat(modifiedTable).isNotEqualTo(defaultTable);

        TableMetadata nonstandard = TableMetadata.builder(SchemaConstants.AUTH_KEYSPACE_NAME, "nonstandard")
                                                 .addPartitionKeyColumn("pk", Int32Type.instance)
                                                 .comment("A non-standard table that should be preserved")
                                                 .params(TableParams.builder().readRepair(ReadRepairStrategy.NONE).build())
                                                 .build();
        KeyspaceMetadata km = KeyspaceMetadata.create(SchemaConstants.AUTH_KEYSPACE_NAME,
                                                      KeyspaceParams.simple(3),
                                                      Tables.of(modifiedTable, nonstandard));

        DistributedSchema schema = DistributedSchema.fromSystemTables(Keyspaces.of(km),
                                                                      Set.of(DatabaseDescriptor.getLocalDataCenter()));
        KeyspaceMetadata merged = schema.getKeyspaceMetadata(SchemaConstants.AUTH_KEYSPACE_NAME);
        assertThat(merged.getTableOrViewNullable(AuthKeyspace.ROLES)).isEqualTo(modifiedTable);
        assertThat(merged.getTableOrViewNullable("nonstandard")).isEqualTo(nonstandard);

        // check all other default tables in the auth keyspace were included in the merged schema
        AuthKeyspace.metadata().tables.forEach(tm -> {
           if (!tm.name.equals(AuthKeyspace.ROLES))
               assertThat(merged.getTableOrViewNullable(tm.name)).isEqualTo(tm);
        });

    }
}
