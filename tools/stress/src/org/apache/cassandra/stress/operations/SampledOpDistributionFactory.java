package org.apache.cassandra.stress.operations;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.StressAction.MeasurementSink;
import org.apache.cassandra.stress.generate.DistributionFactory;
import org.apache.cassandra.stress.generate.DistributionFixed;
import org.apache.cassandra.stress.report.Timer;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

public abstract class SampledOpDistributionFactory<T> implements OpDistributionFactory
{

    final Map<T, Double> ratios;
    final DistributionFactory clustering;
    protected SampledOpDistributionFactory(Map<T, Double> ratios, DistributionFactory clustering)
    {
        this.ratios = ratios;
        this.clustering = clustering;
    }

    protected abstract List<? extends Operation> get(Timer timer, T key, boolean isWarmup);

    public OpDistribution get(boolean isWarmup, MeasurementSink sink)
    {
        List<Pair<Operation, Double>> operations = new ArrayList<>();
        for (Map.Entry<T, Double> ratio : ratios.entrySet())
        {
            List<? extends Operation> ops = get(new Timer(ratio.getKey().toString(), sink),
                                                ratio.getKey(), isWarmup);
            for (Operation op : ops)
                operations.add(new Pair<>(op, ratio.getValue() / ops.size()));
        }
        return new SampledOpDistribution(new EnumeratedDistribution<>(operations), clustering.get());
    }

    public String desc()
    {
        List<T> keys = new ArrayList<>();
        for (Map.Entry<T, Double> ratio : ratios.entrySet())
            keys.add(ratio.getKey());
        return keys.toString();
    }

    public Iterable<OpDistributionFactory> each()
    {
        List<OpDistributionFactory> out = new ArrayList<>();
        for (final Map.Entry<T, Double> ratio : ratios.entrySet())
        {
            out.add(new OpDistributionFactory()
            {
                public OpDistribution get(boolean isWarmup, MeasurementSink sink)
                {
                    List<? extends Operation> ops = SampledOpDistributionFactory.this.get(new Timer(ratio.getKey().toString(), sink),
                                                                                          ratio.getKey(),
                                                                                          isWarmup);
                    if (ops.size() == 1)
                        return new FixedOpDistribution(ops.get(0));
                    List<Pair<Operation, Double>> ratios = new ArrayList<>();
                    for (Operation op : ops)
                        ratios.add(new Pair<>(op, 1d / ops.size()));
                    return new SampledOpDistribution(new EnumeratedDistribution<>(ratios), new DistributionFixed(1));
                }

                public String desc()
                {
                    return ratio.getKey().toString();
                }

                public Iterable<OpDistributionFactory> each()
                {
                    return Collections.<OpDistributionFactory>singleton(this);
                }
            });
        }
        return out;
    }

}
