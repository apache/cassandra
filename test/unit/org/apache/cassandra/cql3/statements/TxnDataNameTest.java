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

import org.apache.cassandra.service.accord.txn.TxnDataName;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.Generators;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.utils.FailingConsumer.orFail;
import static org.quicktheories.QuickTheory.qt;

public class TxnDataNameTest
{
    @Test
    public void serde()
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            qt().forAll(gen()).checkAssert(orFail(name -> {
                out.clear();

                long expectedSize = TxnDataName.serializer.serializedSize(name, 12);
                TxnDataName.serializer.serialize(name, out, 12);
                Assertions.assertThat(out.getLength()).isEqualTo(expectedSize);

                TxnDataName read = TxnDataName.serializer.deserialize(new DataInputBuffer(out.toByteArray()), 12);
                Assertions.assertThat(read).isEqualTo(name);
            }));
        }
    }

    public static Gen<TxnDataName> gen()
    {
        Gen<TxnDataName.Kind> kindGen = SourceDSL.arbitrary().enumValues(TxnDataName.Kind.class);
        Gen<String> symbolGen = Generators.SYMBOL_GEN;
        return rnd -> {
            TxnDataName.Kind kind = kindGen.generate(rnd);
            switch (kind)
            {
                case USER: return TxnDataName.user(symbolGen.generate(rnd));
                case RETURNING: return TxnDataName.returning();
                case AUTO_READ: return new TxnDataName(kind, symbolGen.generate(rnd), symbolGen.generate(rnd), symbolGen.generate(rnd));
                default: throw new IllegalArgumentException("Unknown kind: " + kind);
            }
        };
    }
}