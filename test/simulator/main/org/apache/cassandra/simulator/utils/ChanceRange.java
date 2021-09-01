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

package org.apache.cassandra.simulator.utils;

import java.util.function.ToDoubleFunction;

import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.asm.ChanceSupplier;

public class ChanceRange
{
    public final ToDoubleFunction<RandomSource> distribution;
    public final float min;
    public final float max;

    public ChanceRange(ToDoubleFunction<RandomSource> distribution, float min, float max)
    {
        this.distribution = distribution;
        assert min >= 0 && max <= 1.0;
        this.min = min;
        this.max = max;
    }

    public float select(RandomSource random)
    {
        if (min >= max) return min;
        return (float) ((distribution.applyAsDouble(random) * (max - min)) + min);
    }

    public ChanceSupplier asSupplier(RandomSource random)
    {
        return () -> select(random);
    }
}
