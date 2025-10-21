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

public class EntropyRandomSource implements RandomSource
{
    private final EntropySource delegate;

    public EntropyRandomSource(EntropySource delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public void nextBytes(byte[] bytes)
    {
        for (int i = 0, len = bytes.length; i < len; )
            for (int rnd = nextInt(),
                 n = Math.min(len - i, Integer.SIZE/Byte.SIZE);
                 n-- > 0; rnd >>= Byte.SIZE)
                bytes[i++] = (byte)rnd;
    }

    @Override
    public boolean nextBoolean()
    {
        return delegate.nextBoolean();
    }

    @Override
    public int nextInt()
    {
        return delegate.nextInt();
    }

    @Override
    public long nextLong()
    {
        return ((long) nextInt() << 32) + nextInt();
    }

    @Override
    public float nextFloat()
    {
        return delegate.nextFloat();
    }

    @Override
    public double nextDouble()
    {
        throw new UnsupportedOperationException("TODO: Implement");
    }

    @Override
    public double nextGaussian()
    {
        throw new UnsupportedOperationException("TODO: Implement");
    }

    @Override
    public void setSeed(long seed)
    {
        delegate.seed(seed);
    }

    @Override
    public RandomSource fork()
    {
        return new EntropyRandomSource(delegate.derive());
    }
}
