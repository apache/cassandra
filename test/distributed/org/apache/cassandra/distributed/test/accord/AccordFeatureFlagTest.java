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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.cassandra.db.virtual.AccordVirtualTables;
import org.apache.cassandra.db.virtual.SystemViewsKeyspace;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.AssertionUtils;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.config.DatabaseDescriptor.NO_ACCORD_PAXOS_STRATEGY_WITH_ACCORD_DISABLED_MESSAGE;
import static org.apache.cassandra.cql3.statements.TransactionStatement.TRANSACTIONS_DISABLED_MESSAGE;
import static org.apache.cassandra.schema.SchemaConstants.ACCORD_KEYSPACE_NAME;

public class AccordFeatureFlagTest extends TestBaseImpl
{
    @Test
    public void shouldHideAccordTransactions()  throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK).set("accord_transactions_enabled", "false"))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k int, c int, v int, primary key (k, c))");

            // Any transaction should fail to execute:
            String query = "BEGIN TRANSACTION\n" +
                           "  SELECT * FROM " + KEYSPACE + ".tbl WHERE k=0 AND c=0;\n" +
                           "COMMIT TRANSACTION";
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY))
                      .has(AssertionUtils.isThrowableInstanceof(InvalidRequestException.class))
                      .hasMessage(TRANSACTIONS_DISABLED_MESSAGE);
            
            // The Accord system keyspace should not be present:
            assertEquals("The Accord system keyspace should not exist",
                         Optional.empty(), cluster.get(1).callOnInstance(() -> Schema.instance.getLocalKeyspaces().get(ACCORD_KEYSPACE_NAME)));
            
            // Make sure virtual tables don't exist:
            IIsolatedExecutor.SerializableCallable<Stream<VirtualTable>> hasAccordVirtualTables = 
                    () -> SystemViewsKeyspace.instance.tables().stream().filter(t -> t.getClass().equals(AccordVirtualTables.Epoch.class));
            List<VirtualTable> tables = cluster.get(1).callOnInstance(hasAccordVirtualTables).collect(Collectors.toList());
            assertEquals("No Accord virtual tables should exist", Collections.emptyList(), tables);

            // Make sure we throw if someone tries to coordinate a transaction against the no-op service:
            Assertions.assertThatThrownBy(() -> cluster.get(1).callOnInstance(() -> AccordService.instance().coordinate(null, null)))
                      .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @SuppressWarnings("Convert2MethodRef")
    @Test
    public void shouldFailOnAccordMigrationWithAccordDisabled()  throws IOException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withoutVNodes()
                                      .withConfig(c -> c.with(Feature.NETWORK)
                                                        .set("accord_transactions_enabled", "false")
                                                        .set("legacy_paxos_strategy", "accord")).createWithoutStarting())
        {

            Assertions.assertThatThrownBy(() -> cluster.startup())
                      .has(AssertionUtils.isThrowableInstanceof(ConfigurationException.class))
                      .hasMessage(NO_ACCORD_PAXOS_STRATEGY_WITH_ACCORD_DISABLED_MESSAGE);
        }
    }
}
