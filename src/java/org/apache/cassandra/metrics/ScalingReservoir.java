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
package org.apache.cassandra.metrics;

import com.codahale.metrics.Snapshot;

/**
 * A reservoir that scales the values before updating.
 */
public class ScalingReservoir implements SnapshottingReservoir
{
    private final SnapshottingReservoir delegate;
    private final ScaleFunction scaleFunc;

    public ScalingReservoir(SnapshottingReservoir reservoir, ScaleFunction scaleFunc)
    {
        this.delegate = reservoir;
        this.scaleFunc = scaleFunc;
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public void update(long value)
    {
        delegate.update(scaleFunc.apply(value));
    }

    @Override
    public Snapshot getSnapshot()
    {
        return delegate.getSnapshot();
    }

    @Override
    public Snapshot getPercentileSnapshot()
    {
        return delegate.getPercentileSnapshot();
    }

    /**
     * Scale the input value.
     *
     * Not using {@code java.util.function.Function<Long, Long>} to avoid auto-boxing.
     */
    @FunctionalInterface
    public static interface ScaleFunction
    {
        long apply(long value);
    }
}
