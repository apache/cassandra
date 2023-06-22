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

package org.apache.cassandra.db.compaction.unified;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reservations management for compaction. Defines the two types of reservations, and implements the code for accepting
 * or rejecting compactions to satisfy the reservation requirements.
 */
public abstract class Reservations
{
    public enum Type
    {
        /** The given number of reservations can be used only for the level. */
        PER_LEVEL,
        /** The reservations can be used for the level, or any one below it. */
        LEVEL_OR_BELOW
    }

    private static final Logger logger = LoggerFactory.getLogger(Reservations.class);

    /** Number of compactions to reserve for each level. */
    final int perLevelCount;
    /** Remainder of compactions to be distributed among the levels. */
    final int remainder;
    /** Whether only one compaction over the reservation count is allowed per level. */
    final boolean oneRemainderPerLevel;
    /** Number of compactions already running or selected in each level. */
    final int[] perLevel;

    private Reservations(int totalCount, int[] perLevel, int reservedThreadsTarget)
    {
        this.perLevel = perLevel;

        int levelCount = perLevel.length;
        // Each level has this number of tasks reserved for it.
        perLevelCount = Math.min(totalCount / levelCount, reservedThreadsTarget);
        // The remainder is distributed according to the prioritization.
        remainder = totalCount - perLevelCount * levelCount;
        // If the user requested more than we can give, do not allow more than one extra per level.
        oneRemainderPerLevel = perLevelCount < reservedThreadsTarget;
    }

    public abstract boolean accept(int inLevel);
    public abstract void debugOutput(int selectedCount, int proposedCount, int remaining);

    public static Reservations create(int totalCount, int[] perLevel, int reservedThreadsTarget, Type reservationsType)
    {
        if (reservedThreadsTarget == 0)
            return new Trivial(totalCount, perLevel);
        return reservationsType == Type.PER_LEVEL
               ? new PerLevel(totalCount, perLevel, reservedThreadsTarget)
               : new LevelOrBelow(totalCount, perLevel, reservedThreadsTarget);
    }

    /**
     * Trivial tracker used when there are no reservations. All compactions are accepted.
     */
    private static class Trivial extends Reservations
    {
        private Trivial(int totalCount, int[] perLevel)
        {
            super(totalCount, perLevel, 0);
        }

        @Override
        public boolean accept(int inLevel)
        {
            ++perLevel[inLevel];
            return true;
        }

        @Override
        public void debugOutput(int selectedCount, int proposedCount, int remaining)
        {
            logger.debug("Selected {} compactions (out of {} pending). Compactions per level {} (no reservations) remaining {}.",
                         selectedCount, proposedCount, perLevel, remaining);
        }
    }

    /**
     * Per-level tracker.
     * <p>
     * Reservations are applied by tracking how much of the remainder threads are being used, and only allowing
     * compactions in a level if their number is below the per-level count, or if there is a remainder slot to be given.
     */
    private static class PerLevel extends Reservations
    {
        int remainderDistributed;

        PerLevel(int totalCount, int[] perLevel, int reservedThreadsTarget)
        {
            super(totalCount, perLevel, reservedThreadsTarget);

            remainderDistributed = 0;
            for (int countInLevel : perLevel)
                if (countInLevel > perLevelCount)
                    remainderDistributed += countInLevel - perLevelCount;
        }

        @Override
        public boolean accept(int inLevel)
        {
            if (perLevel[inLevel] >= perLevelCount)
            {
                if (remainderDistributed >= remainder)
                    return false;  // share used up and no remainder to distribute
                if (oneRemainderPerLevel && perLevel[inLevel] > perLevelCount)
                    return false;  // this level is already using up all its share + one
                ++remainderDistributed;
            }
            ++perLevel[inLevel];
            return true;
        }

        @Override
        public void debugOutput(int selectedCount, int proposedCount, int remaining)
        {
            int remainingNonReserved = remainder - remainderDistributed;
            logger.debug("Selected {} compactions (out of {} pending). Compactions per level {} (reservations {}{}) remaining reserved {} non-reserved {}.",
                         selectedCount, proposedCount, perLevel, perLevelCount, oneRemainderPerLevel ? "+1" : "", remaining - remainingNonReserved, remainingNonReserved);
        }
    }

    /**
     * Tracker for the level or below case.
     * <p>
     * For any given level, the reservations are satisfied if the total sum of compactions for the level and all levels
     * above it is at most the product of the number of levels and the per-level count, plus any remainder (up to the
     * number of levels when oneRemainderPerLevel is true).
     * <p>
     * To permit a compaction, we gather this sum for all levels above, and make sure this property will not be violated
     * by adding the new compaction for the current, as well as all levels below it. The latter is necessary because
     * a lower level may have already used up all allocations for this one.
     */
    private static class LevelOrBelow extends Reservations
    {
        LevelOrBelow(int totalCount, int[] perLevel, int reservedThreadsTarget)
        {
            super(totalCount, perLevel, reservedThreadsTarget);
        }

        @Override
        public boolean accept(int inLevel)
        {
            // Limit the sum of the number of threads of any level and all higher to their number
            // times perLevelCount, plus any remainder (up to the number when oneRemainderPerLevel is true).
            int sum = 0;
            int permitted = 0;
            int permittedRemainder = oneRemainderPerLevel ? 0 : remainder;
            int level = perLevel.length - 1;
            // For all higher levels, calculate the total number of threads used and permitted.
            for (; level > inLevel; --level)
            {
                sum += perLevel[level];
                permitted += perLevelCount;
                if (oneRemainderPerLevel && permittedRemainder < remainder)
                    ++permittedRemainder;
            }

            // For this level and all below, check that the limit is not yet hit.
            for (; level >= 0; --level)
            {
                sum += perLevel[level];
                permitted += perLevelCount;
                if (oneRemainderPerLevel && permittedRemainder < remainder)
                    ++permittedRemainder;
                if (sum >= permitted + permittedRemainder)
                    return false; // some lower level used up our share
            }
            ++perLevel[inLevel];
            return true;
        }

        @Override
        public void debugOutput(int selectedCount, int proposedCount, int remaining)
        {
            logger.debug("Selected {} compactions (out of {} pending). Compactions per level {} (reservations level or below {}{}) remaining {}.",
                         selectedCount, proposedCount, perLevel, perLevelCount, oneRemainderPerLevel ? "+1" : "", remaining);
        }
    }
}
