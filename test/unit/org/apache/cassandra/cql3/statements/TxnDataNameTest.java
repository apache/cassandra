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

package org.apache.cassandra.cql3.statements;

import org.junit.Test;

import org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;
import org.quicktheories.impl.Constraint;

import static org.apache.cassandra.service.accord.txn.TxnData.TXN_DATA_NAME_INDEX_MAX;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataNameIndex;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataNameKind;
import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;

public class TxnDataNameTest
{
    @Test
    public void buildAndAccess()
    {
        qt().forAll(gen()).checkAssert(orFail(test -> {
            if (test.index < 0 || test.index > TXN_DATA_NAME_INDEX_MAX)
            {
                try
                {
                    txnDataName(test.kind, test.index);
                    fail("Expect IllegalArgumentException");
                }
                catch (IllegalArgumentException e)
                {
                    // expected
                }
                return;
            }

            int txnDataName = txnDataName(test.kind, test.index);
            assertEquals(test.kind, txnDataNameKind(txnDataName));
            assertEquals(test.index, txnDataNameIndex(txnDataName));
        }));
    }

    @Test
    public void testIndex()
    {
        TxnDataNameKind kind = TxnDataNameKind.values()[TxnDataNameKind.values().length - 1];
        int txnDataName = txnDataName(kind, 0);
        assertEquals(0, txnDataNameIndex(txnDataName));
        txnDataName = txnDataName(kind, TXN_DATA_NAME_INDEX_MAX);
        assertEquals(TXN_DATA_NAME_INDEX_MAX, txnDataNameIndex(txnDataName));
    }

    static class TestData
    {
        final TxnDataNameKind kind;
        final int index;

        public TestData(TxnDataNameKind kind, int index)
        {
            this.kind = kind;
            this.index = index;
        }
    }

    public static Gen<TestData> gen()
    {
        Gen<TxnDataNameKind> kindGen = SourceDSL.arbitrary().enumValues(TxnDataNameKind.class);
        return rnd -> {
            TxnDataNameKind kind = kindGen.generate(rnd);
            int index = (int)(rnd.next(Constraint.zeroToOne()) == 0 ? rnd.next(Constraint.between(0, TXN_DATA_NAME_INDEX_MAX)) : rnd.next(Constraint.between(Integer.MIN_VALUE, Integer.MAX_VALUE)));
            return new TestData(kind, index);
        };
    }
}