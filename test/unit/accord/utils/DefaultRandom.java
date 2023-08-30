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

package accord.utils;

import java.util.Random;

public class DefaultRandom implements RandomSource
{
    private final Random delegate;
    public DefaultRandom()
    {
        this.delegate = new Random();
    }

    public DefaultRandom(long seed)
    {
        this.delegate = new Random(seed);
    }

    @Override
    public void nextBytes(byte[] bytes)
    {
        delegate.nextBytes(bytes);
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
        return delegate.nextLong();
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
    public double nextGaussian()
    {
        return delegate.nextGaussian();
    }

    @Override
    public void setSeed(long seed)
    {
        delegate.setSeed(seed);
    }

    @Override
    public DefaultRandom fork() {
        return new DefaultRandom(nextLong());
    }

    @Override
    public Random asJdkRandom()
    {
        return delegate;
    }
}
