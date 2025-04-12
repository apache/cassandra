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

package org.apache.cassandra.harry.gen.rng;

import java.util.Random;

import org.apache.cassandra.harry.gen.EntropySource;

public class JdkRandomEntropySource implements EntropySource
{
    private final Random rng;

    public JdkRandomEntropySource(long seed)
    {
        this(new Random(seed));
    }

    public JdkRandomEntropySource(Random rng)
    {
        this.rng = rng;
    }

    @Override
    public long next()
    {
        return rng.nextLong();
    }

    @Override
    public void seed(long seed)
    {
        rng.setSeed(seed);
    }

    @Override
    public EntropySource derive()
    {
        return new JdkRandomEntropySource(new Random(rng.nextLong()));
    }

    @Override
    public int nextInt()
    {
        return rng.nextInt();
    }

    /**
     * Generates int between [0, max).
     */
    @Override
    public int nextInt(int max)
    {
        return rng.nextInt(max);
    }

    /**
     * Generates a value between [min, max).
     */
    @Override
    public int nextInt(int min, int max)
    {
        if (min == max)
            return min;
        return rng.nextInt(max - min) + min;
    }

    public long nextLong()
    {
        return rng.nextLong();
    }

    @Override
    public float nextFloat()
    {
        return rng.nextFloat();
    }

    @Override
    public double nextDouble()
    {
        return rng.nextDouble();
    }

    @Override
    public boolean nextBoolean()
    {
        return rng.nextBoolean();
    }
}
