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

package org.apache.cassandra.service.accord;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.Commit;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;
import static org.apache.cassandra.service.accord.AccordTestUtils.wrapInTxn;

public class AccordKeyspaceTest extends CQLTester.InMemory
{
    private static final Ranges GLOBAL_SCOPE = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min(KEYSPACE), AccordRoutingKey.SentinelKey.max(KEYSPACE)));

    @Test
    public void serde()
    {
        AtomicLong now = new AtomicLong();

        String tableName = createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        AccordCommandStore store = AccordTestUtils.createAccordCommandStore(now::incrementAndGet, KEYSPACE, tableName);

        TxnId id = new TxnId(Timestamp.fromValues(1, 42, new Node.Id(1)), Txn.Kind.Read, Routable.Domain.Key);

        Txn txn = createTxn(wrapInTxn(String.format("SELECT * FROM %s.%s WHERE k=? LIMIT 1", KEYSPACE, tableName)), Collections.singletonList(42));

        PartialTxn partialTxn = txn.slice(GLOBAL_SCOPE, true);
        RoutingKey routingKey = partialTxn.keys().get(0).asKey().toUnseekable();
        FullRoute<?> route = partialTxn.keys().toRoute(routingKey);
        Deps deps = new Deps(KeyDeps.none((Keys) txn.keys()), RangeDeps.NONE);
        PartialDeps partialDeps = deps.slice(GLOBAL_SCOPE);


        CommonAttributes.Mutable common = new CommonAttributes.Mutable(id);
        common.partialTxn(partialTxn);
        common.route(route);
        common.partialDeps(partialDeps);
        common.durability(Status.Durability.NotDurable);
        Command.WaitingOn waitingOn = Command.WaitingOn.none(partialDeps);

        Command.Committed committed = Command.SerializerSupport.committed(common, SaveStatus.Committed, id, Ballot.ZERO, Ballot.ZERO, waitingOn);
        AccordSafeCommand safeCommand = new AccordSafeCommand(AccordTestUtils.loaded(id, null));
        safeCommand.set(committed);

        Commit commit = Commit.SerializerSupport.create(id, route.slice(GLOBAL_SCOPE), 1, Commit.Kind.Maximal, id, partialTxn, partialDeps, route, null);
        store.appendToJournal(commit);

        Mutation mutation = AccordKeyspace.getCommandMutation(store, safeCommand, 42);
        mutation.apply();

        Assertions.assertThat(AccordKeyspace.loadCommand(store, id)).isEqualTo(committed);
    }
}