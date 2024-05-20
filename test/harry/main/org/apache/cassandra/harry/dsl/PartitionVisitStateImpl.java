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

package org.apache.cassandra.harry.dsl;

import java.util.Arrays;
import java.util.NavigableSet;

import org.apache.cassandra.harry.ddl.SchemaSpec;

public class PartitionVisitStateImpl implements PartitionVisitState
{
    final long pd;
    final long[] possibleCds;
    final NavigableSet<Long> visitedLts;
    final SchemaSpec schema;
    private final OverridingCkGenerator ckGenerator;

    PartitionVisitStateImpl(long pd, long[] possibleCds, NavigableSet<Long> visitedLts, SchemaSpec schema)
    {
        this.pd = pd;
        this.possibleCds = possibleCds;
        this.visitedLts = visitedLts;
        this.schema = schema;
        this.ckGenerator = (OverridingCkGenerator) schema.ckGenerator;
    }


    /**
     * Ensures that exactly one of the clustering keys will have given values.
     */
    @Override
    public void ensureClustering(Object[] overrides)
    {
        long cd = findCdForOverride(overrides);
        ckGenerator.override(cd, overrides);
    }

    @Override
    public void overrideClusterings(Object[][] overrides)
    {
        assert possibleCds.length == overrides.length;
        Arrays.sort(overrides, this::compareCds);
        for (int i = 0; i < overrides.length; i++)
            ckGenerator.override(possibleCds[i], overrides[i]);
    }

    @Override
    public long pd()
    {
        return pd;
    }

    long findCdForOverride(Object[] ck)
    {
        int low = 0;
        int high = possibleCds.length - 1;

        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            long midEl = possibleCds[mid];
            int cmp = compareCds(ck, midEl);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                throw new IllegalStateException("This value is already present");
        }

        return possibleCds[Math.min(possibleCds.length - 1, low)];
    }

    private int compareCds(Object[] v1, long cd2)
    {
        Object[] v2 = schema.ckGenerator.inflate(cd2);
        return compareCds(v1, v2);
    }

    private int compareCds(Object[] v1, Object[] v2)
    {
        assert v1.length == v2.length : String.format("Values should be of same length: %d != %d\n%s\n%s",
                                                      v1.length, v2.length, Arrays.toString(v1), Arrays.toString(v2));

        for (int i = 0; i < v1.length; i++)
        {
            int res = ((Comparable) v2[i]).compareTo(v1[i]);
            if (res != 0)
            {
                if (schema.clusteringKeys.get(i).type.isReversed())
                    res = res * -1;

                return res;
            }
        }
        return 0;
    }
}