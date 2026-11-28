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

package org.apache.cassandra.service.accord.interop;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.MockedConstruction;

import accord.api.Result;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.PersistTxn;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Routable.Domain;
import accord.primitives.Txn;
import accord.primitives.TxnId;

import org.apache.cassandra.service.accord.txn.TxnDataResult;
import org.apache.cassandra.service.accord.txn.TxnResult;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;

public class FailedReadPersistenceTest
{
    private final Node node = mock(Node.class, RETURNS_DEEP_STUBS);
    private final Txn txn = mock(Txn.class);
    private final TxnId id = new TxnId(1, 1, Txn.Kind.Read, Domain.Key, new Node.Id(1));

    private final CoordinationAdapter<Result> adapter
            = new AccordInteropAdapter.AccordInteropFactory(null).get(id, CoordinationAdapter.Factory.Kind.Standard);

    @Test
    public void failedReadPersistsTheSameMarkerAsRemoteDelivery()
    {
        AtomicReference<Object> persisted = new AtomicReference<>();
        try (MockedConstruction<PersistTxn> construction = mockConstruction(PersistTxn.class,
                (instance, context) -> persisted.set(context.arguments().get(10))))
        {
            // ExecuteTxn.onDone uses these arguments after a failed read when recoverReads is false.
            adapter.persist(node, null, null, null, null, null, Ballot.ZERO, CoordinationFlags.none(),
                            id, txn, id, Deps.NONE, null, null, false, null);
            assertEquals(1, construction.constructed().size());
            assertSame("A local Apply must receive the marker used by ApplySerializers", TxnResult.PERSISTABLE, persisted.get());
        }
    }

    @Test
    public void missingClientResultIsNotReplacedWithASuccess()
    {
        assertThatThrownBy(() ->
            adapter.persist(node, null, null, null, null, null, Ballot.ZERO, CoordinationFlags.none(),
                            id, txn, id, Deps.NONE, null, null, false, (value, failure) -> fail("No successful client result exists")))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void successfulReadRetainsItsClientResult()
    {
        TxnDataResult result = new TxnDataResult(1);
        AtomicReference<Result> reply = new AtomicReference<>();
        AtomicReference<Object> persisted = new AtomicReference<>();
        try (MockedConstruction<PersistTxn> construction = mockConstruction(PersistTxn.class,
                (instance, context) -> persisted.set(context.arguments().get(10))))
        {
            adapter.persist(node, null, null, null, null, null, Ballot.ZERO, CoordinationFlags.none(),
                            id, txn, id, Deps.NONE, null, result, false, (value, failure) -> {
                                assertNull(failure);
                                reply.set(value);
                            });
            assertEquals(1, construction.constructed().size());
            assertSame(result, reply.get());
            assertSame(TxnResult.PERSISTABLE, persisted.get());
        }
    }
}
