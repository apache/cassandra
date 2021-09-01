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

import java.util.Arrays;

public abstract class ActionSchedulersRandom implements ActionSchedulers
{
    public enum SplitRange { NO, DEADLINE, ISOLATE }

    protected final RandomSource random;
    protected final float delayChance;
    protected final float dropChance;
    protected final float timeoutChance;
    protected final SplitRange splitRange;

    abstract class Inner extends ActionScheduler
    {
        final double min, range;

        Inner(double min, double range)
        {
            this.min = min;
            this.range = range;
        }

        @Override
        protected boolean decideIfDrop()
        {
            return random.decide(dropChance);
        }

        @Override
        protected boolean decideIfDelay()
        {
            return random.decide(delayChance);
        }

        @Override
        public boolean decideIfPermitTimeouts()
        {
            return !random.decide(timeoutChance);
        }

        @Override
        protected ActionScheduler next()
        {
            switch (splitRange)
            {
                default: throw new AssertionError();
                case NO:
                    return this;
                case ISOLATE:
                case DEADLINE:
                    return inner(min, range * random.uniformDouble());
            }
        }

        @Override
        public void attachTo(ActionList actions)
        {
            if (actions instanceof ActionSequence && ((ActionSequence) actions).orderOn.isOrdered())
            {
                double[] next = new double[actions.size()];
                for (int i = 0 ; i < next.length ; ++i)
                    next[i] = range * random.uniformDouble();

                Arrays.sort(next);
                for (int i = 0 ; i < next.length ; ++i)
                {
                    ActionScheduler inner;
                    switch (splitRange)
                    {
                        default: throw new AssertionError();
                        case NO:
                            inner = this;
                            break;
                        case DEADLINE:
                            inner = inner(min, next[i]);
                            break;
                        case ISOLATE:
                            inner = inner(i == 0 ? min : min + next[i - 1],
                                          i == 0 ? next[i] : next[i] - next[i - 1]);
                    }

                    actions.get(i).setScheduler(inner);
                }
            }
            else
            {
                actions.forEach(this);
            }
        }

        @Override
        public void accept(Action action)
        {
            action.setScheduler(next());
        }

        protected abstract ActionScheduler inner(double min, double range);
    }

    public ActionSchedulersRandom(RandomSource random, float delayChance, float dropChance, float timeoutChance, SplitRange splitRange)
    {
        this.random = random;
        this.delayChance = delayChance;
        this.dropChance = dropChance;
        this.timeoutChance = timeoutChance;
        this.splitRange = splitRange;
    }

    protected abstract ActionScheduler root();

    @Override
    public void attachTo(ActionList actions)
    {
        root().attachTo(actions);
    }
}

