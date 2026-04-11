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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.systems.NonInterceptible;

import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.REQUIRED;

class OnClusterConsensusMigrations extends Action
{
    private final KeyspaceActions actions;
    private final int migrations;

    OnClusterConsensusMigrations(KeyspaceActions actions, int migrations)
    {
        super("Performing " + migrations + " consensus migrations", NONE, NONE);
        this.actions = actions;
        this.migrations = migrations;
    }

    @Override
    public ActionList performSimple()
    {
        IInvokableInstance instance1 = actions.cluster.get(1);
        String keyspace = actions.keyspace;
        String table = actions.table;
        TransactionalMode transactionalMode = TransactionalMode.valueOf(
        NonInterceptible.apply(REQUIRED, () -> instance1.unsafeCallOnThisThread(() -> Schema.instance.getTableMetadata(keyspace, table).params.transactionalMode.toString())));

        // Just migrate back and forth
        List<Action> result = new ArrayList<>();
        for (int i = 0; i < migrations; i++)
        {

            TransactionalMode targetMode = TransactionalMode.full;
            if (transactionalMode == TransactionalMode.full)
                targetMode = TransactionalMode.off;
            result.add(new OnClusterMigrateConsensus(actions, transactionalMode, targetMode));
            transactionalMode = targetMode;
        }

        return ActionList.of(result).setStrictlySequential();
    }
}
