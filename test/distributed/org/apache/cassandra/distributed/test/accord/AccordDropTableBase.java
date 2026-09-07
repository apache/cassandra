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

import com.google.common.base.Throwables;

import org.assertj.core.api.Assertions;

import accord.local.CommandStores;
import accord.local.ExecutionContext;
import accord.local.Node;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.execution.SaferCommandStore;

import static org.apache.cassandra.config.DatabaseDescriptor.getPartitioner;
import static org.apache.cassandra.service.accord.AccordService.getBlocking;

public class AccordDropTableBase extends TestBaseImpl
{
    protected static void addChaos(Cluster cluster, int example)
    {
        cluster.filters().reset();
        cluster.filters().verbs(Verb.ACCORD_APPLY_REQ.id).from(1).to(3).drop();
    }

    protected static void doTxn(Cluster cluster, int step)
    {
        int stepId = step % 3;
        int partitionId = step % 10;
        int coordinatorId = (step % 2) + 1; // avoid node3 as it can't get applies from node1, so leads to user errors
        ICoordinator coordinator = cluster.coordinator(coordinatorId);
        switch (stepId)
        {
            case 0: // insert
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("INSERT INTO %s.tbl(pk, v) VALUES (?, ?);")), ConsistencyLevel.ANY, partitionId, step));
                break;
            case 1: // insert + read
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("UPDATE %s.tbl SET v+=1 WHERE pk=?;")), ConsistencyLevel.ANY, partitionId));
                break;
            case 2: // read
                retry(3, () -> coordinator.executeWithResult(wrapInTxn(withKeyspace("SELECT * FROM %s.tbl WHERE pk=?")), ConsistencyLevel.ANY, partitionId));
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    protected static void retry(int maxAttempts, Runnable fn)
    {
        for (int i = 0; i < maxAttempts; i++)
        {
            try
            {
                fn.run();
            }
            catch (Throwable t)
            {
                if (i == (maxAttempts - 1))
                    throw t;
            }
        }
    }

    protected static TableId createTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl(pk int PRIMARY KEY, v int) WITH transactional_mode='full'"));
        return ClusterUtils.tableId(cluster, KEYSPACE, "tbl");
    }

    protected void dropKeyspace(Cluster cluster)
    {
        // drop keyspace should be rejected as there is an accord table... so validate that is true then do both
        try
        {
            cluster.schemaChange(withKeyspace("DROP KEYSPACE %s"));
        }
        catch (Throwable t)
        {
            Assertions.assertThat(Throwables.getRootCause(t))
                      .hasMessage("Cannot drop keyspace 'distributed_test_keyspace' as it contains accord tables. (distributed_test_keyspace.tbl)");
        }

        // now do it for real
        dropTable(cluster);
        cluster.schemaChange(withKeyspace("DROP KEYSPACE %s"));
    }

    protected static void dropTable(Cluster cluster)
    {
        cluster.schemaChange(withKeyspace("DROP TABLE %s.tbl"));
    }

    protected static void validateAccord(Cluster cluster, TableId id)
    {
        String s = id.toString();
        for (IInvokableInstance inst : cluster)
        {
            inst.runOnInstance(() -> {
                TableId tableId = TableId.fromString(s);
                AccordService accord = (AccordService) AccordService.instance();
                TxnId syntheticTxnId = new TxnId(TxnId.MAX_EPOCH, 0, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, new Node.Id(1));
                ExecutionContext ctx = ExecutionContext.unsequencedReadWrite(syntheticTxnId, Ranges.single(TokenRange.fullRange(tableId, getPartitioner())), "Test");
                CommandStores stores = accord.node().commandStores();
                for (int storeId : stores.ids())
                {
                    AccordCommandStore store = (AccordCommandStore) stores.forId(storeId);
                    getBlocking(store.chain(ctx, input -> {
                        SaferCommandStore safe = (SaferCommandStore) input;
                        for (SafeCommandsForKey safeCfk : safe.safeCommandsForKeys())
                        {
                            CommandsForKey cfk = safeCfk.current();
                            CommandsForKey.TxnInfo minUndecided = cfk.minUndecidedManaged();
                            if (minUndecided != null)
                                throw new AssertionError("Undecided txn: " + minUndecided);
                            TxnId next = cfk.nextWaitingToApply();
                            if (next != null)
                                throw new AssertionError("Unapplied txn: " + next);
                        }
                        return null;
                    }));
                }
            });
        }
    }
}
