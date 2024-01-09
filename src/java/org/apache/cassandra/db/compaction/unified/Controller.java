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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

/**
* The controller provides compaction parameters to the unified compaction strategy
*/
public class Controller
{
    protected static final Logger logger = LoggerFactory.getLogger(Controller.class);

    /**
     * The scaling parameters W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    final static String SCALING_PARAMETERS_OPTION = "scaling_parameters";
    private final static String DEFAULT_SCALING_PARAMETERS =
        CassandraRelevantProperties.UCS_SCALING_PARAMETER.getString();

    /**
     * The minimum sstable size. Sharded writers split sstables over shard only if they are at least as large as the
     * minimum size.
     */
    static final String MIN_SSTABLE_SIZE_OPTION = "min_sstable_size";

    private static final String DEFAULT_MIN_SSTABLE_SIZE = CassandraRelevantProperties.UCS_MIN_SSTABLE_SIZE.getString();

    /**
     * Override for the flush size in MB. The database should be able to calculate this from executing flushes, this
     * should only be necessary in rare cases.
     */
    static final String FLUSH_SIZE_OVERRIDE_OPTION = "flush_size_override";

    static final String BASE_SHARD_COUNT_OPTION = "base_shard_count";
    /**
     * Default base shard count, used when a base count is not explicitly supplied. This value applies as long as the
     * table is not a system one, and directories are not defined.
     *
     * For others a base count of 1 is used as system tables are usually small and do not need as much compaction
     * parallelism, while having directories defined provides for parallelism in a different way.
     */
    public static final int DEFAULT_BASE_SHARD_COUNT =
        CassandraRelevantProperties.UCS_BASE_SHARD_COUNT.getInt();

    static final String TARGET_SSTABLE_SIZE_OPTION = "target_sstable_size";
    public static final long DEFAULT_TARGET_SSTABLE_SIZE =
        CassandraRelevantProperties.UCS_TARGET_SSTABLE_SIZE.getSizeInBytes();
    static final long MIN_TARGET_SSTABLE_SIZE = 1L << 20;

    /**
     * Provision for growth of the constructed SSTables as the size of the data grows. By default, the target SSTable
     * size is fixed for all levels. In some scenarios it may be better to reduce the overall number of SSTables when
     * the data size becomes larger to avoid using too much memory and processing for the corresponding structures.
     * The setting enables such control and determines how much we reduce the growth of the number of split points as
     * the data size grows. The number specifies the SSTable growth part, and the difference from 1 is the shard count
     * growth component, which is a multiplier applied to the logarithm of the data size, before it is rounded and
     * applied as an exponent in the number of split points. In other words, the given value applies as a negative
     * exponent in the calculation of the number of split points.
     * <p>
     * Using 0 applies no correction to the number of split points, resulting in SSTables close to the
     * target size. Setting this number to 1 will make UCS never split beyong the base shard count. Using 0.5 will
     * make the number of split points a square root of the required number for the target SSTable size, making
     * the number of split points and the size of SSTables grow in lockstep as the density grows. Using
     * 0.333 (the default) makes the sstable growth the cubic root of the density growth, i.e. the SSTable size
     * grows with the square root of the growth of the shard count.
     * <p>
     * For example, given a data size of 1TiB on the top density level and 1GiB target size with base shard count of 1,
     * growth 0 would result in 1024 SSTables of ~1GiB each, 0.333 would result in 128 SSTables of ~8 GiB each,
     * 0.5 would yield 32 SSTables of ~32GiB each, and 1 would yield 1 SSTable of 1TiB.
     * <p>
     * Note that this correction only applies after the base shard count is reached, so for the above example with
     * base count of 4, the number of SSTables will be 4 (~256GiB each) for a growth value of 1, 128 (~8GiB each) for
     * a growth value of 0.333, and 64 (~16GiB each) for a growth value of 0.5.
     */
    static final String SSTABLE_GROWTH_OPTION = "sstable_growth";
    private static final double DEFAULT_SSTABLE_GROWTH = CassandraRelevantProperties.UCS_SSTABLE_GROWTH.getDouble();

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    static final double DEFAULT_SURVIVAL_FACTOR =
        CassandraRelevantProperties.UCS_SURVIVAL_FACTOR.getDouble();
    static final double[] DEFAULT_SURVIVAL_FACTORS = new double[] { DEFAULT_SURVIVAL_FACTOR };

    /**
     * The maximum number of sstables to compact in one operation.
     *
     * The default is 32, which aims to keep the length of operations under control and prevent accummulation of
     * sstables while compactions are taking place.
     *
     * If the fanout factor is larger than the maximum number of sstables, the strategy will ignore the latter.
     */
    static final String MAX_SSTABLES_TO_COMPACT_OPTION = "max_sstables_to_compact";

    static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION = "unsafe_aggressive_sstable_expiration";
    static final boolean ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION =
        CassandraRelevantProperties.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION.getBoolean();
    static final boolean DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = false;

    static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION = "expired_sstable_check_frequency_seconds";

    /** The maximum exponent for shard splitting. The maximum number of shards is this number the base count shifted this many times left. */
    static final int MAX_SHARD_SHIFT = 20;
    /** The maximum splitting factor for shards. The maximum number of shards is this number multiplied by the base count. */
    static final double MAX_SHARD_SPLIT = Math.scalb(1, MAX_SHARD_SHIFT);

    /**
     * Overlap inclusion method. NONE for participating sstables only (not recommended), SINGLE to only include sstables
     * that overlap with participating (LCS-like, higher concurrency during upgrades but some double compaction),
     * TRANSITIVE to include overlaps of overlaps (likely to trigger whole level compactions, safest).
     */
    static final String OVERLAP_INCLUSION_METHOD_OPTION = "overlap_inclusion_method";
    static final Overlaps.InclusionMethod DEFAULT_OVERLAP_INCLUSION_METHOD =
        CassandraRelevantProperties.UCS_OVERLAP_INCLUSION_METHOD.getEnum(Overlaps.InclusionMethod.TRANSITIVE);

    protected final ColumnFamilyStore cfs;
    protected final MonotonicClock clock;
    private final int[] scalingParameters;
    protected final double[] survivalFactors;
    protected volatile long minSSTableSize;
    protected final long flushSizeOverride;
    protected volatile long currentFlushSize;
    protected final int maxSSTablesToCompact;
    protected final long expiredSSTableCheckFrequency;
    protected final boolean ignoreOverlapsInExpirationCheck;

    protected final int baseShardCount;

    protected final double targetSSTableSize;

    protected final double sstableGrowthModifier;

    static final double INVERSE_SQRT_2 = Math.sqrt(0.5);

    private static final double INVERSE_LOG_2 = 1.0 / Math.log(2);

    protected final Overlaps.InclusionMethod overlapInclusionMethod;

    Controller(ColumnFamilyStore cfs,
               MonotonicClock clock,
               int[] scalingParameters,
               double[] survivalFactors,
               long minSSTableSize,
               long flushSizeOverride,
               int maxSSTablesToCompact,
               long expiredSSTableCheckFrequency,
               boolean ignoreOverlapsInExpirationCheck,
               int baseShardCount,
               double targetSStableSize,
               double sstableGrowthModifier,
               Overlaps.InclusionMethod overlapInclusionMethod)
    {
        this.cfs = cfs;
        this.clock = clock;
        this.scalingParameters = scalingParameters;
        this.survivalFactors = survivalFactors;
        this.minSSTableSize = minSSTableSize;
        this.flushSizeOverride = flushSizeOverride;
        this.currentFlushSize = flushSizeOverride;
        this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(expiredSSTableCheckFrequency, TimeUnit.SECONDS);
        this.baseShardCount = baseShardCount;
        this.targetSSTableSize = targetSStableSize;
        this.overlapInclusionMethod = overlapInclusionMethod;
        this.sstableGrowthModifier = sstableGrowthModifier;

        if (maxSSTablesToCompact <= 0)
            maxSSTablesToCompact = Integer.MAX_VALUE;

        this.maxSSTablesToCompact = maxSSTablesToCompact;

        if (ignoreOverlapsInExpirationCheck && !ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION)
        {
            logger.warn("Not enabling aggressive SSTable expiration, as the system property '" +
                        CassandraRelevantProperties.ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION.name() +
                        "' is set to 'false'. " +
                        "Set it to 'true' to enable aggressive SSTable expiration.");
        }
        this.ignoreOverlapsInExpirationCheck = ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION && ignoreOverlapsInExpirationCheck;
    }

    /**
     * @return the scaling parameter W
     * @param index
     */
    public int getScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < scalingParameters.length ? scalingParameters[index] : scalingParameters[scalingParameters.length - 1];
    }

    @Override
    public String toString()
    {
        return String.format("Controller, m: %s, o: %s, Ws: %s",
                             FBUtilities.prettyPrintBinary(targetSSTableSize, "B", ""),
                             Arrays.toString(survivalFactors),
                             printScalingParameters(scalingParameters));
    }

    public int getFanout(int index) {
        int W = getScalingParameter(index);
        return UnifiedCompactionStrategy.fanoutFromScalingParameter(W);
    }

    public int getThreshold(int index) {
        int W = getScalingParameter(index);
        return UnifiedCompactionStrategy.thresholdFromScalingParameter(W);
    }

    /**
     * Calculate the number of shards to split the local token space in for the given SSTable density.
     * This is calculated as a power-of-two multiple of baseShardCount, so that the expected size of resulting SSTables
     * is between sqrt(0.5) and sqrt(2) times the target size, which is calculated from targetSSTableSize to grow
     * at the given sstableGrowthModifier of the exponential growth of the density.
     * <p>
     * Additionally, if a minimum SSTable size is set, we can go below the baseShardCount when that would result in
     * SSTables smaller than that minimum. Note that in the case of a non-power-of-two base count, we will only
     * split to divisors of baseShardCount.
     * <p>
     * Note that to get the SSTables resulting from this splitting within the bounds, the density argument must be
     * normalized to the span that is being split. In other words, if no disks are defined, the density should be
     * scaled by the token coverage of the locally-owned ranges. If multiple data directories are defined, the density
     * should be scaled by the token coverage of the respective data directory. That is, localDensity = size / span,
     * where the span is normalized so that span = 1 when the data covers the range that is being split.
     */
    public int getNumShards(double localDensity)
    {
        int shards;
        // Check the minimum size first.
        if (minSSTableSize > 0)
        {
            double count = localDensity / minSSTableSize;
            // Minimum size only applies if it is smaller than the base count.
            // Note: the minimum size cannot be larger than the target size's minimum.
            if (count < baseShardCount)
            {
                // Make it a power of two, rounding down so that sstables are greater in size than the min.
                // Setting the bottom bit to 1 ensures the result is at least 1.
                // If baseShardCount is not a power of 2, split only to powers of two that are divisors of baseShardCount so boundaries match higher levels
                shards = Math.min(Integer.highestOneBit((int) count | 1), baseShardCount & -baseShardCount);
                if (logger.isDebugEnabled())
                    logger.debug("Shard count {} for density {}, {} times min size {}",
                                 shards,
                                 FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                                 localDensity / minSSTableSize,
                                 FBUtilities.prettyPrintBinary(minSSTableSize, "B", " "));

                return shards;
            }
        }

        if (sstableGrowthModifier == 1)
        {
            shards = baseShardCount;
            logger.debug("Shard count {} for density {} in fixed shards mode",
                         shards,
                         FBUtilities.prettyPrintBinary(localDensity, "B", " "));
            return shards;
        }
        else if (sstableGrowthModifier == 0)
        {
            // How many we would have to aim for the target size. Divided by the base shard count, so that we can ensure
            // the result is a multiple of it by multiplying back below. Adjusted by sqrt(0.5) to calculate the split
            // points needed for the minimum size.
            double count = localDensity / (targetSSTableSize * INVERSE_SQRT_2 * baseShardCount);
            if (count > MAX_SHARD_SPLIT)
                count = MAX_SHARD_SPLIT;
            assert !(count < 0);    // Must be positive, 0 or NaN, which should translate to baseShardCount

            // Make it a power of two multiple of the base count so that split points for lower levels remain split points for higher.
            // The conversion to int and highestOneBit round down, for which we compensate by using the sqrt(0.5) multiplier
            // already applied in the count.
            // Setting the bottom bit to 1 ensures the result is at least baseShardCount.
            shards = baseShardCount * Integer.highestOneBit((int) count | 1);

            if (logger.isDebugEnabled())
                logger.debug("Shard count {} for density {}, {} times target {}",
                             shards,
                             FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                             localDensity / targetSSTableSize,
                             FBUtilities.prettyPrintBinary(targetSSTableSize, "B", " "));
            return shards;
        }
        else
        {
            // How many we would have to aim for the target size. Divided by the base shard count, so that we can ensure
            // the result is a multiple of it by multiplying back below.
            double count = localDensity / (targetSSTableSize * baseShardCount);
            // Take a logarithm of the count (in base 2), and adjust it by the given growth modifier.
            // Adjust by 0.5 to round the exponent so that the result falls between targetSSTableSize * sqrt(0.5) and
            // targetSSTableSize * sqrt(2). Finally, make sure the exponent is at least 0 and not greater than the
            // fixed maximum.
            // Note: This code also works correctly for the special cases of sstableGrowthModifier == 0 and 1,
            //       but the above code avoids the imprecise floating point arithmetic for these common cases.
            // Note: We use log instead of getExponent because we also need the non-integer part of the logarithm
            //       in order to apply the growth modifier correctly.
            final double countLog = Math.log(count);
            double pow = countLog * INVERSE_LOG_2 * (1 - sstableGrowthModifier) + 0.5;
            if (pow >= MAX_SHARD_SHIFT)
                shards = baseShardCount << MAX_SHARD_SHIFT;
            else if (pow >= 0)
                shards = baseShardCount << (int) pow;
            else
                shards = baseShardCount;    // this also covers the case of pow == NaN

            if (logger.isDebugEnabled())
            {
                long targetSize = (long) (targetSSTableSize * Math.exp(countLog * sstableGrowthModifier));
                logger.debug("Shard count {} for density {}, {} times target {}",
                             shards,
                             FBUtilities.prettyPrintBinary(localDensity, "B", " "),
                             localDensity / targetSize,
                             FBUtilities.prettyPrintBinary(targetSize, "B", " "));
            }
            return shards;
        }
    }

    /**
     * @return the survival factor o
     * @param index
     */
    public double getSurvivalFactor(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < survivalFactors.length ? survivalFactors[index] : survivalFactors[survivalFactors.length - 1];
    }

    /**
     * Return the flush sstable size in bytes.
     *
     * This is usually obtained from the observed sstable flush sizes, refreshed when it differs significantly
     * from the current values.
     * It can also be set by the user in the options.
     *
     * @return the flush size in bytes.
     */
    public long getFlushSizeBytes()
    {
        if (flushSizeOverride > 0)
            return flushSizeOverride;

        double envFlushSize = cfs.metric.flushSizeOnDisk.get();
        if (currentFlushSize == 0 || Math.abs(1 - (currentFlushSize / envFlushSize)) > 0.5)
        {
            // The current size is not initialized, or it differs by over 50% from the observed.
            // Use the observed size rounded up to a whole megabyte.
            currentFlushSize = ((long) (Math.ceil(Math.scalb(envFlushSize, -20)))) << 20;
        }
        return currentFlushSize;
    }

    /**
     * @return whether is allowed to drop expired SSTables without checking if partition keys appear in other SSTables.
     * Same behavior as in TWCS.
     */
    public boolean getIgnoreOverlapsInExpirationCheck()
    {
        return ignoreOverlapsInExpirationCheck;
    }

    public long getExpiredSSTableCheckFrequency()
    {
        return expiredSSTableCheckFrequency;
    }

    public static Controller fromOptions(ColumnFamilyStore cfs, Map<String, String> options)
    {
        int[] Ws = parseScalingParameters(options.getOrDefault(SCALING_PARAMETERS_OPTION, DEFAULT_SCALING_PARAMETERS));

        long flushSizeOverride = FBUtilities.parseHumanReadableBytes(options.getOrDefault(FLUSH_SIZE_OVERRIDE_OPTION,
                                                                                          "0MiB"));
        int maxSSTablesToCompact = Integer.parseInt(options.getOrDefault(MAX_SSTABLES_TO_COMPACT_OPTION, "0"));
        long expiredSSTableCheckFrequency = options.containsKey(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION)
                ? Long.parseLong(options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION))
                : DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS;
        boolean ignoreOverlapsInExpirationCheck = options.containsKey(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION)
                ? Boolean.parseBoolean(options.get(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION))
                : DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION;

        int baseShardCount;
        if (options.containsKey(BASE_SHARD_COUNT_OPTION))
        {
            baseShardCount = Integer.parseInt(options.get(BASE_SHARD_COUNT_OPTION));
        }
        else
        {
            baseShardCount = DEFAULT_BASE_SHARD_COUNT;
        }

        long targetSStableSize = options.containsKey(TARGET_SSTABLE_SIZE_OPTION)
                ? FBUtilities.parseHumanReadableBytes(options.get(TARGET_SSTABLE_SIZE_OPTION))
                : DEFAULT_TARGET_SSTABLE_SIZE;

        long minSSTableSize = options.containsKey(MIN_SSTABLE_SIZE_OPTION)
                ? FBUtilities.parseHumanReadableBytes(options.get(MIN_SSTABLE_SIZE_OPTION))
                : FBUtilities.parseHumanReadableBytes(DEFAULT_MIN_SSTABLE_SIZE);

        double sstableGrowthModifier = DEFAULT_SSTABLE_GROWTH;
        if (options.containsKey(SSTABLE_GROWTH_OPTION))
            sstableGrowthModifier = FBUtilities.parsePercent(options.get(SSTABLE_GROWTH_OPTION));

        Overlaps.InclusionMethod inclusionMethod = options.containsKey(OVERLAP_INCLUSION_METHOD_OPTION)
                ? Overlaps.InclusionMethod.valueOf(options.get(OVERLAP_INCLUSION_METHOD_OPTION).toUpperCase())
                : DEFAULT_OVERLAP_INCLUSION_METHOD;

        return new Controller(cfs,
                              MonotonicClock.Global.preciseTime,
                              Ws,
                              DEFAULT_SURVIVAL_FACTORS,
                              minSSTableSize,
                              flushSizeOverride,
                              maxSSTablesToCompact,
                              expiredSSTableCheckFrequency,
                              ignoreOverlapsInExpirationCheck,
                              baseShardCount,
                              targetSStableSize,
                              sstableGrowthModifier,
                              inclusionMethod);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        options = new HashMap<>(options);
        String s;

        s = options.remove(SCALING_PARAMETERS_OPTION);
        if (s != null)
            parseScalingParameters(s);

        s = options.remove(BASE_SHARD_COUNT_OPTION);
        if (s != null)
        {
            try
            {
                int numShards = Integer.parseInt(s);
                if (numShards <= 0)
                    throw new ConfigurationException(String.format("Invalid configuration, %s should be positive: %d",
                                                                   BASE_SHARD_COUNT_OPTION,
                                                                   numShards));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s",
                                                               s,
                                                               BASE_SHARD_COUNT_OPTION), e);
            }
        }

        // preserve the configuration for later use during min_sstable_size.
        long targetSSTableSize = DEFAULT_TARGET_SSTABLE_SIZE;
        s = options.remove(TARGET_SSTABLE_SIZE_OPTION);
        if (s != null)
        {
            try
            {
                targetSSTableSize = FBUtilities.parseHumanReadableBytes(s);
                if (targetSSTableSize < MIN_TARGET_SSTABLE_SIZE)
                {
                    throw new ConfigurationException(String.format("%s %s is not acceptable, size must be at least %s",
                                                                   TARGET_SSTABLE_SIZE_OPTION,
                                                                   s,
                                                                   FBUtilities.prettyPrintMemory(MIN_TARGET_SSTABLE_SIZE)));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s %s is not a valid size in bytes: %s",
                                                               TARGET_SSTABLE_SIZE_OPTION,
                                                               s,
                                                               e.getMessage()),
                                                 e);
            }
        }

        s = options.remove(FLUSH_SIZE_OVERRIDE_OPTION);
        if (s != null)
        {
            try
            {
                long flushSize = FBUtilities.parseHumanReadableBytes(s);
                if (flushSize < MIN_TARGET_SSTABLE_SIZE)
                    throw new ConfigurationException(String.format("%s %s is not acceptable, size must be at least %s",
                                                                   FLUSH_SIZE_OVERRIDE_OPTION,
                                                                   s,
                                                                   FBUtilities.prettyPrintMemory(MIN_TARGET_SSTABLE_SIZE)));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s %s is not a valid size in bytes: %s",
                                                               FLUSH_SIZE_OVERRIDE_OPTION,
                                                               s,
                                                               e.getMessage()),
                                                 e);
            }
        }

        s = options.remove(MAX_SSTABLES_TO_COMPACT_OPTION);
        if (s != null)
        {
             try
             {
                 Integer.parseInt(s); // values less than or equal to 0 enable the default
             }
             catch (NumberFormatException e)
             {
                 throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s",
                                                                s,
                                                                MAX_SSTABLES_TO_COMPACT_OPTION),
                                                  e);
             }
        }
        s = options.remove(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION);
        if (s != null)
        {
            try
            {
                long expiredSSTableCheckFrequency = Long.parseLong(s);
                if (expiredSSTableCheckFrequency <= 0)
                    throw new ConfigurationException(String.format("Invalid configuration, %s should be positive: %d",
                                                                   EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION,
                                                                   expiredSSTableCheckFrequency));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a parsable long (base10) for %s",
                                                               s,
                                                               EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION),
                                                 e);
            }
        }

        s = options.remove(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION);
        if (s != null && !s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format("%s should either be 'true' or 'false', not %s",
                                                           ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, s));
        }

        s = options.remove(OVERLAP_INCLUSION_METHOD_OPTION);
        if (s != null)
        {
            try
            {
                Overlaps.InclusionMethod.valueOf(s.toUpperCase());
            }
            catch (IllegalArgumentException e)
            {
                throw new ConfigurationException(String.format("Invalid overlap inclusion method %s. The valid options are %s.",
                                                               s,
                                                               Arrays.toString(Overlaps.InclusionMethod.values())));
            }
        }

        s = options.remove(MIN_SSTABLE_SIZE_OPTION);
        if (s != null)
        {
            try
            {
                long sizeInBytes = FBUtilities.parseHumanReadableBytes(s);
                // zero is a valid option to disable feature
                if (sizeInBytes < 0)
                    throw new ConfigurationException(String.format("Invalid configuration, %s should be greater than or equal to 0 (zero)",
                                                                   MIN_SSTABLE_SIZE_OPTION));
                int limit = (int) Math.ceil(targetSSTableSize * INVERSE_SQRT_2);
                if (sizeInBytes >= limit)
                    throw new ConfigurationException(String.format("Invalid configuration, %s (%s) should be less than the target size minimum: %s",
                                                                   MIN_SSTABLE_SIZE_OPTION,
                                                                   FBUtilities.prettyPrintMemory(sizeInBytes),
                                                                   FBUtilities.prettyPrintMemory(limit)));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid size in bytes for %s",
                                                               s,
                                                               MIN_SSTABLE_SIZE_OPTION),
                                                 e);
            }
        }

        s = options.remove(SSTABLE_GROWTH_OPTION);
        if (s != null)
        {
            try
            {
                double targetSSTableGrowth  = FBUtilities.parsePercent(s);
                if (targetSSTableGrowth < 0 || targetSSTableGrowth > 1)
                {
                    throw new ConfigurationException(String.format("%s %s must be between 0 and 1",
                                                                   SSTABLE_GROWTH_OPTION,
                                                                   s));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s is not a valid number between 0 and 1: %s",
                                                               SSTABLE_GROWTH_OPTION,
                                                               e.getMessage()),
                                                 e);
            }
        }

        return options;
    }

    // The methods below are implemented here (rather than directly in UCS) to aid testability.

    public double getBaseSstableSize(int F)
    {
        // The compaction hierarchy should start at a minimum size which is close to the typical flush size, with
        // some leeway to make sure we don't overcompact when flushes end up a little smaller.
        // The leeway should be less than 1/F, though, to make sure we don't overshoot the boundary combining F-1
        // sources instead of F.
        // Note that while we have not had flushes, the size will be 0 and we will use 1MB as the flush size. With
        // fixed and positive W this should not hurt us, as the hierarchy will be in multiples of F and will still
        // result in the same buckets, but for negative W or hybrid strategies this may cause temporary overcompaction.
        // If this is a concern, the flush size override should be used to avoid it until DB-4401.
        return Math.max(1 << 20, getFlushSizeBytes()) * (1.0 - 0.9 / F);
    }

    public double getMaxLevelDensity(int index, double minSize)
    {
        return Math.floor(minSize * getFanout(index) * getSurvivalFactor(index));
    }

    public double maxThroughput()
    {
        double compactionThroughputMbPerSec = DatabaseDescriptor.getCompactionThroughputMebibytesPerSec();
        if (compactionThroughputMbPerSec <= 0)
            return Double.MAX_VALUE;
        return Math.scalb(compactionThroughputMbPerSec, 20);
    }

    public int maxConcurrentCompactions()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    public int maxSSTablesToCompact()
    {
        return maxSSTablesToCompact;
    }

    /**
     * Random number generator to be used for the selection of tasks.
     * Replaced by some tests.
     */
    public Random random()
    {
        return ThreadLocalRandom.current();
    }

    /**
     * Return the overlap inclusion method to use when combining overlap sections into a bucket. For example, with
     * SSTables A(0, 5), B(2, 9), C(6, 12), D(10, 12) whose overlap sections calculation returns [AB, BC, CD],
     *   - NONE means no sections are to be merged. AB, BC and CD will be separate buckets, compactions AB, BC and CD
     *     will be added separately, thus some SSTables will be partially used / single-source compacted, likely
     *     to be recompacted again with the next selected bucket.
     *   - SINGLE means only overlaps of the sstables in the selected bucket will be added. AB+BC will be one bucket,
     *     and CD will be another (as BC is already used). A middle ground of sorts, should reduce overcompaction but
     *     still has some.
     *   - TRANSITIVE means a transitive closure of overlapping sstables will be selected. AB+BC+CD will be in the same
     *     bucket, selected compactions will apply to all overlapping sstables and no overcompaction will be done, at
     *     the cost of reduced compaction parallelism and increased length of the operation.
     * TRANSITIVE is the default and makes most sense. NONE is a closer approximation to operation of legacy UCS.
     * The option is exposed for experimentation.
     */
    public Overlaps.InclusionMethod overlapInclusionMethod()
    {
        return overlapInclusionMethod;
    }

    public static int[] parseScalingParameters(String str)
    {
        String[] vals = str.split(",");
        int[] ret = new int[vals.length];
        for (int i = 0; i < vals.length; i++)
        {
            String value = vals[i].trim();
            int W = UnifiedCompactionStrategy.parseScalingParameter(value);
            ret[i] = W;
        }

        return ret;
    }

    public static String printScalingParameters(int[] parameters)
    {
        StringBuilder builder = new StringBuilder();
        int i;
        for (i = 0; i < parameters.length - 1; ++i)
        {
            builder.append(UnifiedCompactionStrategy.printScalingParameter(parameters[i]));
            builder.append(", ");
        }
        builder.append(UnifiedCompactionStrategy.printScalingParameter(parameters[i]));
        return builder.toString();
    }
}
