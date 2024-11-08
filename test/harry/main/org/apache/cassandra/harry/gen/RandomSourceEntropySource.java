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

import accord.utils.RandomSource;

public class RandomSourceEntropySource implements EntropySource
{
    private final RandomSource delegate;

    public RandomSourceEntropySource(RandomSource delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public long next()
    {
        return delegate.nextLong();
    }

    @Override
    public void seed(long seed)
    {
        delegate.setSeed(seed);
    }

    @Override
    public EntropySource derive()
    {
        return new RandomSourceEntropySource(delegate.fork());
    }

    @Override
    public int nextInt()
    {
        return delegate.nextInt();
    }

    @Override
    public int nextInt(int max)
    {
        return delegate.nextInt(max);
    }

    @Override
    public int nextInt(int min, int max)
    {
        return delegate.nextInt(min, max);
    }

    @Override
    public float nextFloat()
    {
        return delegate.nextFloat();
    }

    @Override
    public double nextDouble()
    {
        return delegate.nextDouble();
    }

    @Override
    public boolean nextBoolean()
    {
        return delegate.nextBoolean();
    }
}
