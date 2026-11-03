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

package org.apache.cassandra.io.sstable.metadata;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

/**
 * See CASSANDRA-21841
 */
public final class PartitionKeyUnion
{
    private ICardinality first;
    private HyperLogLogPlus union;

    public void add(ICardinality sketch) throws CardinalityMergeException
    {
        if (first == null)
        {
            first = sketch;
            return;
        }

        if (union == null)
        {
            union = new HyperLogLogPlus(MetadataCollector.CARDINALITY_PRECISION);
            union.addAll((HyperLogLogPlus) first);
        }

        union.addAll((HyperLogLogPlus) sketch);
    }

    public boolean isEmpty()
    {
        return first == null;
    }

    public long cardinality()
    {
        if (union != null)
            return union.cardinality();
        return first == null ? 0 : first.cardinality();
    }
}
