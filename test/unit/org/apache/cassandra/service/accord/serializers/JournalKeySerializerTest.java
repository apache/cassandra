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

package org.apache.cassandra.service.accord.serializers;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.harry.dsl.TestRunner;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;

public class JournalKeySerializerTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void testOrder()
    {
        Node.Id node = new Node.Id(1);
        Generator<Txn.Kind> kindGen = Generators.enumValues(Txn.Kind.class);
        Generator<Routable.Domain> domainGen = Generators.enumValues(Routable.Domain.class);
        Generator<JournalKey.Type> keyTypeGen = Generators.enumValues(JournalKey.Type.class);

        Generator<JournalKey> keyGen = rng -> {
            TxnId txnId = new TxnId(rng.nextLong(0, Timestamp.MAX_EPOCH + 1),
                                    rng.nextLong(0, Long.MAX_VALUE),
                                    kindGen.generate(rng),
                                    domainGen.generate(rng),
                                    node);
            return new JournalKey(txnId, keyTypeGen.generate(rng), rng.nextInt(100));
        };
        TestRunner.test(keyGen, keyGen, (key1, key2) -> {
            DecoratedKey dk1 = AccordKeyspace.JournalColumns.decorate(key1);
            DecoratedKey dk2 = AccordKeyspace.JournalColumns.decorate(key2);
            Assert.assertEquals(String.format("Sort mismatch for\n%s (%s) \n%s (%s) ", key1, dk1, key2, dk2),
                                dk1.compareTo(dk2) >= 0 ? 1 : -1,
                                JournalKey.SUPPORT.compare(key1, key2) >= 0 ? 1 : -1);
        });
    }
}
