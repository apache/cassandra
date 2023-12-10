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

    public long next()
    {
        return rng.nextLong();
    }

    public void seed(long seed)
    {
        rng.setSeed(seed);
    }

    public EntropySource derive()
    {
        return new JdkRandomEntropySource(new Random(rng.nextLong()));
    }

    public int nextInt()
    {
        return rng.nextInt();
    }

    public int nextInt(int max)
    {
        return rng.nextInt(max);
    }

    public int nextInt(int min, int max)
    {
        return rng.nextInt(max) + min;
    }

    public long nextLong()
    {
        return rng.nextLong();
    }

    public float nextFloat()
    {
        return rng.nextFloat();
    }

    public boolean nextBoolean()
    {
        return rng.nextBoolean();
    }
}
