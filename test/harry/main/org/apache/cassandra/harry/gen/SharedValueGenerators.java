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

package org.apache.cassandra.harry.gen;

import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.harry.gen.Bijections.Bijection;

public class SharedValueGenerators<PartitionKey, ClusteringKey> extends ValueGenerators<PartitionKey, ClusteringKey>
{
    protected final PartitionValues<ClusteringKey> partitionValues;

    public SharedValueGenerators(Bijection<PartitionKey> pkGen,

                                 Bijection<ClusteringKey> ckGen,
                                 Accessor<ClusteringKey> ckAccessor,

                                 List<? extends Bijection<? extends Object>> regularColumnGens,
                                 List<? extends Bijection<? extends Object>> staticColumnGens,

                                 List<Comparator<Object>> ckComparators,
                                 List<Comparator<Object>> regularComparators,
                                 List<Comparator<Object>> staticComparators)
    {
        super(pkGen);
        this.partitionValues = new PartitionValues<>(ckGen, ckAccessor, regularColumnGens, staticColumnGens, ckComparators, regularComparators, staticComparators);
    }

    public Bijection<PartitionKey> pkGen()
    {
        return pkGen;
    }

    @Override
    public PartitionValues<ClusteringKey> forPd(long pd)
    {
        return partitionValues;
    }
}