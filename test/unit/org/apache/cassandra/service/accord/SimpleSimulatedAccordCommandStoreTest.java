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

import org.junit.Test;

import accord.local.PreLoadContext;
import accord.local.SaveStatus;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class SimpleSimulatedAccordCommandStoreTest extends SimulatedAccordCommandStoreTestBase
{
    @Test
    public void emptyTxns()
    {
        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                for (int i = 0, examples = 100; i < examples; i++)
                {
                    TxnId id = AccordGens.txnIds().next(rs);
                    instance.process(PreLoadContext.contextFor(id), (safe) -> {
                        var safeCommand = safe.get(id, id, Ranges.EMPTY);
                        var command = safeCommand.current();
                        Assertions.assertThat(command.saveStatus()).isEqualTo(SaveStatus.Uninitialised);
                        return null;
                    });
                }
            }

        });
    }
}
