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

package org.apache.cassandra.simulator.cluster;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.migration.ConsensusTableMigration;
import org.apache.cassandra.service.consensus.migration.TableMigrationState;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.systems.NonInterceptible;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.REQUIRED;

public class OnClusterAssertMigrationComplete extends Action
{
    private final KeyspaceActions actions;

    public OnClusterAssertMigrationComplete(KeyspaceActions actions)
    {
        super("Validate consensus migration completed", NONE, NONE);
        this.actions = actions;
    }

    @Override
    public ActionList performSimple()
    {
        String keyspace = actions.keyspace;
        String table = actions.table;
        for (IInvokableInstance instance : actions.cluster)
        {
            if (instance.isShutdown())
            {
                continue;
            }
            NonInterceptible.apply(REQUIRED, () -> instance.unsafeCallOnThisThread(() ->
                {
                    TableMetadata tm = Schema.instance.getTableMetadata(keyspace, table);
                    TableMigrationState tms = ConsensusTableMigration.getTableMigrationState(tm.id);
                    checkState(tms == null, "There should be no table migration state after migration completes, migrating ranges %s migrated ranges %s", tms == null ? null : tms.migratingRanges, tms == null ? null : tms.migratedRanges);
                    checkState(tm.params.transactionalMigrationFrom == TransactionalMigrationFromMode.none, "transactionalMigrationFrom should be none after migration completes");
                    return null;
                }));
        }
       return ActionList.empty();
    }
}
