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

package org.apache.cassandra.simulator;

public class ActionSchedulersRandomWalk extends ActionSchedulersRandom
{
    class Walk extends Inner
    {
        final double modifier;
        final double direction;

        Walk(double min, double range, double modifier, double direction)
        {
            super(min, range);
            this.modifier = modifier;
            this.direction = direction;
        }

        @Override
        protected double delayed(Action action)
        {
            return min + random.uniformDouble() * range * modifier;
        }

        @Override
        protected ActionScheduler inner(double min, double range)
        {
            // TODO: this needs to be thought through some more
            double direction = ((random.uniformDouble() * 4d) - 1d) * this.direction;
            if (Math.abs(direction) < 0.00001d) direction = 0.00001d;
            double modifier = Math.max(0, Math.min(1d, this.modifier + (this.modifier * direction)));
            return new Walk(min, range, direction, modifier);
        }
    }

    public ActionSchedulersRandomWalk(RandomSource random, float delayChance, float dropChance, float timeoutChance, SplitRange splitRange)
    {
        super(random, delayChance, dropChance, timeoutChance, splitRange);
    }

    @Override
    protected ActionScheduler root()
    {
        return new Walk(0d, 0d, 0.5d, 0.1d);
    }
}

