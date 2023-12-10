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

import org.apache.cassandra.harry.gen.EntropySource;

public class PcgRSUFast implements EntropySource
{
    private long state;
    private long step;

    /**
     * Stream number of the rng.
     */
    private final long stream;

    public PcgRSUFast(long seed, long streamNumber)
    {
        this.stream = (streamNumber << 1) | 1; // 2* + 1
        seed(seed);
    }

    @Override
    public void seed(long seed)
    {
        state = RngUtils.xorshift64star(seed) + stream;
    }

    public void advance(long steps)
    {
        this.step += steps;
        this.state = PCGFastPure.advanceState(state, steps, stream);
    }

    protected void nextStep()
    {
        state = PCGFastPure.nextState(state, stream);
        step++;
    }

    public void seek(long step)
    {
        advance(step - this.step);
    }

    @Override
    public EntropySource derive()
    {
        return new PcgRSUFast(PCGFastPure.nextState(state, stream), stream);
    }

    @Override
    public long next()
    {
        nextStep();
        return PCGFastPure.shuffle(state);
    }

    public long nextAt(long step)
    {
        seek(step);
        return next();
    }

    @Override
    public int nextInt()
    {
        return RngUtils.asInt(next());
    }

    @Override
    public int nextInt(int max)
    {
        return RngUtils.asInt(next(), max);
    }

    @Override
    public int nextInt(int min, int max)
    {
        return RngUtils.asInt(next(), min, max);
    }

    @Override
    public long nextLong(long min, long max)
    {
        return RngUtils.trim(next(), min, max);
    }

    @Override
    public float nextFloat()
    {
        return RngUtils.asFloat(next());
    }

    @Override
    public boolean nextBoolean()
    {
        return RngUtils.asBoolean(next());
    }

    public long distance(long generated)
    {
        return PCGFastPure.distance(state, PCGFastPure.unshuffle(generated), stream);
    }

    public long distance(PcgRSUFast other)
    {
        // Check if they are the same stream...
        if (stream != other.stream)
        {
            throw new IllegalArgumentException("Can not compare generators with different " +
                                               "streams. Those generators will never converge");
        }

        return PCGFastPure.distance(state, other.state, stream);
    }
}