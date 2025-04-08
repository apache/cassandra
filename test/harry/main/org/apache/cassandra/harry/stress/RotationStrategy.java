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

package org.apache.cassandra.harry.stress;

import java.util.Arrays;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;

/**
 * Rotation strategy is a way to control a number of active partitions.
 *
 * Rotation strategy has a target size, but will vary the number of partition to maintain it within a distribution over time.
 */
public interface RotationStrategy extends Generator<RotationStrategy.PartitionAction[]>
{
    int targetSize();

    /**
     * Determines whether a partition switch should occur at the given logical timestamp.
     * Implementations track the last LTS at which a switch occurred and compare against
     * the configured interval.
     */
    boolean shouldSwitch(long lts);

    // TODO (required): control a total number of partitions
    enum PartitionAction
    {
        REPLACE_WITH_NEW,      // Replace partition: rotate current one out, and pick a new partition in its place
        REPLACE_WITH_VISITED,  // Replace partition: rotate current one out, and pick an already visited partition in its place
    }

    /**
     * A trivial random rotation strategy, would attempt to keep the size close to the target, but
     * has no bias or gives any guarantees.
     */
    class RandomRotationStrategy implements RotationStrategy
    {
        private static final Generator<PartitionAction> gen = Generators.pick(PartitionAction.REPLACE_WITH_VISITED, PartitionAction.REPLACE_WITH_NEW);
        private final PartitionAction[] EMPTY = new PartitionAction[] {};
        private final int targetSize;
        private final int switchInterval;
        private long lastSwitchLts = -1;

        public RandomRotationStrategy(int targetSize)
        {
            this(targetSize, 500);
        }

        public RandomRotationStrategy(int targetSize, int switchInterval)
        {
            this.targetSize = targetSize;
            this.switchInterval = switchInterval;
        }

        @Override
        public int targetSize()
        {
            return targetSize;
        }

        @Override
        public boolean shouldSwitch(long lts)
        {
            if (lastSwitchLts < 0 || lts - lastSwitchLts >= switchInterval)
            {
                lastSwitchLts = lts;
                return true;
            }
            return false;
        }

        @Override
        public PartitionAction[] generate(EntropySource rng)
        {
            // TODO (required): make configurable
//            if (rng.nextBoolean())
//                return EMPTY;

            PartitionAction[] actions = new PartitionAction[rng.nextInt(5, 10)];
            for (int i = 0; i < actions.length; i++)
                actions[i] = gen.generate(rng);
            return actions;
        }

        @Override
        public String toString()
        {
            return String.format("random(target=%d, switchInterval=%d)", targetSize, switchInterval);
        }
    }

    class FixedRotationStrategy implements RotationStrategy
    {
        private final int replaceWithNew;
        private final int replaceWithVisited;
        private final int targetSize;
        private final int switchInterval;
        private long lastSwitchLts = -1;

        public FixedRotationStrategy(int targetSize, int replaceWithNew, int replaceWithVisited)
        {
            this(targetSize, replaceWithNew, replaceWithVisited, 500);
        }

        public FixedRotationStrategy(int targetSize, int replaceWithNew, int replaceWithVisited, int switchInterval)
        {
            this.replaceWithNew = replaceWithNew;
            this.replaceWithVisited = replaceWithVisited;
            this.targetSize = targetSize;
            this.switchInterval = switchInterval;
        }

        @Override
        public int targetSize()
        {
            return targetSize;
        }

        @Override
        public boolean shouldSwitch(long lts)
        {
            if (lastSwitchLts < 0 || lts - lastSwitchLts >= switchInterval)
            {
                lastSwitchLts = lts;
                return true;
            }
            return false;
        }

        @Override
        public PartitionAction[] generate(EntropySource rng)
        {
            PartitionAction[] actions = new PartitionAction[replaceWithNew + replaceWithVisited];
            Arrays.fill(actions, 0, replaceWithNew, PartitionAction.REPLACE_WITH_NEW);
            Arrays.fill(actions, replaceWithNew, replaceWithNew + replaceWithVisited, PartitionAction.REPLACE_WITH_VISITED);
            return actions;
        }

        @Override
        public String toString()
        {
            return String.format("fixed(target=%d, replaceWithNew=%d, replaceWithVisited=%d, switchInterval=%d)",
                                 targetSize, replaceWithNew, replaceWithVisited, switchInterval);
        }
    }
}