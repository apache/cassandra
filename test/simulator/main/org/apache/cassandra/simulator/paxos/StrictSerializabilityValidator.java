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

import javax.annotation.Nullable;

import accord.verify.StrictSerializabilityVerifier;
import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;

public class StrictSerializabilityValidator implements HistoryValidator
{
    private final StrictSerializabilityVerifier verifier;
    private final IntIntMap pkToIndex;
    private final int[] indexToPk;

    public StrictSerializabilityValidator(int[] primaryKeys)
    {
        this.verifier = new StrictSerializabilityVerifier(primaryKeys.length);
        pkToIndex = new IntIntHashMap(primaryKeys.length);
        indexToPk = new int[primaryKeys.length];
        for (int i = 0; i < primaryKeys.length; i++)
        {
            pkToIndex.put(primaryKeys[i], i);
            indexToPk[i] = primaryKeys[i];
        }
    }

    @Override
    public Checker witness(int start, int end)
    {
        verifier.begin();
        return new Checker()
        {
            @Override
            public void read(int pk, int id, int count, int[] seq)
            {
                verifier.witnessRead(get(pk), seq);
            }

            @Override
            public void write(int pk, int id, boolean success)
            {
                verifier.witnessWrite(get(pk), id);
            }

            @Override
            public void close()
            {
                convertHistoryViolation(() -> verifier.apply(start, end));
            }
        };
    }

    @Override
    public void print(@Nullable Integer pk)
    {
        if (pk == null) verifier.print();
        else verifier.print(get(pk));
    }

    private int get(int pk)
    {
        if (pkToIndex.containsKey(pk))
            return pkToIndex.get(pk);
        throw new IllegalArgumentException("Unknown pk=" + pk);
    }

    private void convertHistoryViolation(Runnable fn)
    {
        try
        {
            fn.run();
        }
        catch (accord.verify.HistoryViolation e)
        {
            if (!(e.primaryKey() >= 0 && e.primaryKey() < indexToPk.length))  throw new IllegalArgumentException("Unable to find primary key by index " + e.primaryKey());
            int pk = indexToPk[e.primaryKey()];
            HistoryViolation v = new HistoryViolation(pk, e.getMessage());
            v.setStackTrace(e.getStackTrace());
            throw v;
        }
    }

    public static class Factory implements HistoryValidator.Factory
    {
        public static final Factory instance = new Factory();

        @Override
        public HistoryValidator create(int[] partitions)
        {
            return new StrictSerializabilityValidator(partitions);
        }
    }
}
