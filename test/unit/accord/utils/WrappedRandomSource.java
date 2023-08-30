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

class WrappedRandomSource implements RandomSource
{
    private final Random random;

    WrappedRandomSource(Random random)
    {
        this.random = random;
    }

    @Override
    public Random asJdkRandom()
    {
        return random;
    }

    @Override
    public void nextBytes(byte[] bytes)
    {
        random.nextBytes(bytes);
    }

    @Override
    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    @Override
    public int nextInt()
    {
        return random.nextInt();
    }

    @Override
    public int nextInt(int maxExclusive)
    {
        return random.nextInt(maxExclusive);
    }

    @Override
    public long nextLong()
    {
        return random.nextLong();
    }

    @Override
    public float nextFloat()
    {
        return random.nextFloat();
    }

    @Override
    public double nextDouble()
    {
        return random.nextDouble();
    }

    @Override
    public double nextGaussian()
    {
        return random.nextGaussian();
    }

    @Override
    public void setSeed(long seed)
    {
        random.setSeed(seed);
    }

    @Override
    public RandomSource fork()
    {
        return new WrappedRandomSource(new Random(nextLong()));
    }
}
