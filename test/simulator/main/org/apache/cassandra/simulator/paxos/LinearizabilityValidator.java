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

package org.apache.cassandra.simulator.paxos;

import java.util.function.Consumer;
import javax.annotation.Nullable;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

public class LinearizabilityValidator implements HistoryValidator
{
    private final IntObjectMap<HistoryChecker> historyCheckers;

    public LinearizabilityValidator(int[] primaryKeys)
    {
        historyCheckers = new IntObjectHashMap<>(primaryKeys.length);
        for (int primaryKey : primaryKeys)
            historyCheckers.put(primaryKey, new HistoryChecker(primaryKey));
    }

    @Override
    public Checker witness(int start, int end)
    {
        return new Checker()
        {
            @Override
            public void read(int pk, int id, int count, int[] seq)
            {
                get(pk).witness(id, seq, start, end);
            }

            @Override
            public void write(int pk, int id, boolean success)
            {
                get(pk).applied(id, start, end, success);
            }
        };
    }

    @Override
    public void print(@Nullable Integer pk)
    {
        if (pk == null) historyCheckers.values().forEach((Consumer<ObjectCursor<HistoryChecker>>) c -> c.value.print());
        else historyCheckers.get(pk).print();
    }

    private HistoryChecker get(int pk)
    {
        HistoryChecker checker = historyCheckers.get(pk);
        if (checker == null)
            throw new NullPointerException("Unable to find checker for pk=" + pk);
        return checker;
    }

    public static class Factory implements HistoryValidator.Factory
    {
        public static final Factory instance = new Factory();

        @Override
        public HistoryValidator create(int[] partitions)
        {
            return new LinearizabilityValidator(partitions);
        }
    }
}
