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

/**
 * Immutable / pure implementaiton of PCG.
 * <p>
 * Base algorithm translated from C/C++ code:
 * https://github.com/imneme/pcg-c
 * https://github.com/imneme/pcg-cpp
 * <p>
 * Original library developed by Melissa O'Neill (oneill@pcg-random.org)
 */
public class PCGFastPure
{
    private static final long NEXT_MULTIPLIER = 6364136223846793005L;

    private static final long XORSHIFT_MULTIPLIER = Long.parseUnsignedLong("12605985483714917081");
    private static final long XORSHIFT_UNMULTIPLIER = Long.parseUnsignedLong("15009553638781119849");

    public static long advance(long generated, long steps, long stream)
    {
        return shuffle(advanceState(unshuffle(generated), steps, stream));
    }

    public static long streamIncrement(long stream)
    {
        return (stream << 1) | 1;
    }

    public static long advanceState(long state, long steps, long stream)
    {
        long acc_mult = 1;
        long acc_plus = 0;

        long cur_plus = streamIncrement(stream);
        long cur_mult = NEXT_MULTIPLIER;

        while (Long.compareUnsigned(steps, 0) > 0)
        {
            if ((steps & 1) == 1)
            {
                acc_mult *= cur_mult;
                acc_plus = acc_plus * cur_mult + cur_plus;
            }
            cur_plus *= (cur_mult + 1);
            cur_mult *= cur_mult;
            steps = Long.divideUnsigned(steps, 2);
        }
        return (acc_mult * state) + acc_plus;
    }

    public static long next(long state, long stream)
    {
        return shuffle(nextState(unshuffle(state), stream));
    }

    public static long previous(long state, long stream)
    {
        return advance(state, -1, stream);
    }

    public static long nextState(long state, long stream)
    {
        return (state * NEXT_MULTIPLIER) + streamIncrement(stream);
    }

    public static long distance(long curState, long newState, long stream)
    {
        if (curState == newState)
            return 0;

        long curPlus = streamIncrement(stream);
        long curMult = NEXT_MULTIPLIER;

        long bit = 1L;
        long distance = 0;

        while (curState != newState)
        {
            if ((curState & bit) != (newState & bit))
            {
                curState = curState * curMult + curPlus;
                distance |= bit;
            }
            assert ((curState & bit) == (newState & bit));
            bit <<= 1;
            curPlus = (curMult + 1) * curPlus;
            curMult *= curMult;
        }

        return distance;
    }

    public static long shuffle(long state)
    {
        long word = ((state >>> ((state >>> 59) + 5)) ^ state) * XORSHIFT_MULTIPLIER;
        return (word >>> 43) ^ word;
    }

    public static long unshuffle(long shuffled)
    {
        long word = shuffled;
        word = unxorshift(word, 43);
        word *= XORSHIFT_UNMULTIPLIER;
        word = unxorshift(word, (int) (word >>> 59) + 5);
        return word;
    }

    public static long unxorshift(long x, int shift)
    {
        return unxorshift(x, 64, shift);
    }

    public static long unxorshift(long x, int bits, int shift)
    {
        if (2 * shift >= bits)
            return x ^ (x >>> shift);

        long lowmask1 = (1L << (bits - shift * 2)) - 1L;
        long bottomBits = x & lowmask1;
        long topBits = (x ^ (x >>> shift)) & ~lowmask1;
        x = topBits | bottomBits;

        long lowmask2 = (1L << (bits - shift)) - 1L;
        bottomBits = unxorshift(x & lowmask2, bits - shift, shift) & lowmask1;
        return topBits | bottomBits;
    }
}