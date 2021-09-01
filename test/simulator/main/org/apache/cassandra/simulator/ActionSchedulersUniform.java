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

// TODO (now): loguniform version
public class ActionSchedulersUniform extends ActionSchedulersRandom
{
    class Uniform extends Inner
    {
        Uniform(double min, double range)
        {
            super(min, range);
        }

        @Override
        protected double delayed(Action action)
        {
            return random.uniformDouble() * range + min;
        }

        @Override
        protected ActionScheduler inner(double min, double range)
        {
            return new Uniform(min, range);
        }
    }

    public ActionSchedulersUniform(RandomSource random, float delayChance, float dropChance, float timeoutChance, SplitRange splitRange)
    {
        super(random, delayChance, dropChance, timeoutChance, splitRange);
    }

    @Override
    protected ActionScheduler root()
    {
        return new Uniform(0d, 1d);
    }
}

