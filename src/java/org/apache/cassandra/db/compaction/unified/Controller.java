/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.cassandra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.Overlaps;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;

/**
* The controller provides compaction parameters to the unified compaction strategy
*/
public class Controller
{
    protected static final Logger logger = LoggerFactory.getLogger(Controller.class);

    static final String PREFIX = "unified_compaction.";

    /**
     * The scaling parameters W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    final static String STATIC_SCALING_PARAMETERS_OPTION = "static_scaling_parameters";
    private final static String DEFAULT_STATIC_SCALING_PARAMETERS = System.getProperty(PREFIX + STATIC_SCALING_PARAMETERS_OPTION, "T4");

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
    public static final int DEFAULT_BASE_SHARD_COUNT = Integer.parseInt(System.getProperty(PREFIX + BASE_SHARD_COUNT_OPTION, "4"));

    static final String TARGET_SSTABLE_SIZE_OPTION = "target_sstable_size";
    public static final double DEFAULT_TARGET_SSTABLE_SIZE = FBUtilities.parseHumanReadable(System.getProperty(PREFIX + TARGET_SSTABLE_SIZE_OPTION, "1GiB"), null, "B");
    static final double MIN_TARGET_SSTABLE_SIZE = 1L << 20;

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    static final double DEFAULT_SURVIVAL_FACTOR = Double.parseDouble(System.getProperty(PREFIX + "survival_factor", "1"));
    static final double[] DEFAULT_SURVIVAL_FACTORS = new double[] { DEFAULT_SURVIVAL_FACTOR };

    /**
     * The maximum number of sstables to compact in one operation.
     *
     * This is expected to be large and never be reached, but compaction going very very late may cause the accumulation
     * of thousands and even tens of thousands of sstables which may cause problems if compacted in one long operation.
     * The default is chosen to be half of the maximum permitted space overhead when the source sstables are of the
     * minimum sstable size.
     *
     * If the fanout factor is larger than the maximum number of sstables, the strategy will ignore the latter.
     */
    static final String MAX_SSTABLES_TO_COMPACT_OPTION = "max_sstables_to_compact";

    static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION = "unsafe_aggressive_sstable_expiration";
    static final String ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY = Config.PROPERTY_PREFIX + "allow_unsafe_aggressive_sstable_expiration";
    static final boolean ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = Boolean.parseBoolean(System.getProperty(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY));
    static final boolean DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION = false;

    static final int DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS = 60 * 10;
    static final String EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION = "expired_sstable_check_frequency_seconds";

    /**
     * True if L0 data may be coming from different replicas.
     */
    public static final String SHARED_STORAGE = "shared_storage";

    /** The maximum splitting factor for shards. The maximum number of shards is this number multiplied by the base count. */
    static final double MAX_SHARD_SPLIT = 1048576;

    /**
     * Overlap inclusion method. NONE for participating sstables only (not recommended), SINGLE to only include sstables
     * that overlap with participating (LCS-like, higher concurrency during upgrades but some double compaction),
     * TRANSITIVE to include overlaps of overlaps (likely to trigger whole level compactions, safest).
     */
    static final String OVERLAP_INCLUSION_METHOD_OPTION = "overlap_inclusion_method";
    static final Overlaps.InclusionMethod DEFAULT_OVERLAP_INCLUSION_METHOD =
        Overlaps.InclusionMethod.valueOf(System.getProperty(PREFIX + OVERLAP_INCLUSION_METHOD_OPTION,
                                                            Overlaps.InclusionMethod.TRANSITIVE.toString()).toUpperCase());

    protected final ColumnFamilyStore cfs;
    protected final MonotonicClock clock;
    private final int[] scalingParameters;
    protected final double[] survivalFactors;
    protected final long flushSizeOverride;
    protected volatile long currentFlushSize;
    protected final int maxSSTablesToCompact;
    protected final long expiredSSTableCheckFrequency;
    protected final boolean ignoreOverlapsInExpirationCheck;

    protected final int baseShardCount;

    protected final double targetSSTableSizeMin;

    protected final Overlaps.InclusionMethod overlapInclusionMethod;

    Controller(ColumnFamilyStore cfs,
               MonotonicClock clock,
               int[] scalingParameters,
               double[] survivalFactors,
               long flushSizeOverride,
               int maxSSTablesToCompact,
               long expiredSSTableCheckFrequency,
               boolean ignoreOverlapsInExpirationCheck,
               int baseShardCount,
               double targetSStableSize,
               Overlaps.InclusionMethod overlapInclusionMethod)
    {
        this.cfs = cfs;
        this.clock = clock;
        this.scalingParameters = scalingParameters;
        this.survivalFactors = survivalFactors;
        this.flushSizeOverride = flushSizeOverride;
        this.currentFlushSize = flushSizeOverride;
        this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(expiredSSTableCheckFrequency, TimeUnit.SECONDS);
        this.baseShardCount = baseShardCount;
        this.targetSSTableSizeMin = targetSStableSize * Math.sqrt(0.5);
        this.overlapInclusionMethod = overlapInclusionMethod;

        if (maxSSTablesToCompact <= 0)
            maxSSTablesToCompact = Integer.MAX_VALUE;

        this.maxSSTablesToCompact = maxSSTablesToCompact;

        if (ignoreOverlapsInExpirationCheck && !ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION)
        {
            logger.warn("Not enabling aggressive SSTable expiration, as the system property '" + ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY + "' is set to 'false'. " +
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
                             FBUtilities.prettyPrintBinary(targetSSTableSizeMin, "B", ""),
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
     * Calculate the number of shards to split the local token space in for the given sstable density.
     * This is calculated as a power-of-two multiple of baseShardCount, so that the expected size of resulting sstables
     * is between targetSSTableSizeMin and 2*targetSSTableSizeMin (in other words, sqrt(0.5) * targetSSTableSize and
     * sqrt(2) * targetSSTableSize), with a minimum of baseShardCount shards for smaller sstables.
     */
    public int getNumShards(double density)
    {
        // How many we would have to aim for the target size. Divided by the base shard count, so that we can ensure
        // the result is a multiple of it by multiplying back below.
        double count = density / (targetSSTableSizeMin * baseShardCount);
        if (count > MAX_SHARD_SPLIT)
            count = MAX_SHARD_SPLIT;
        assert !(count < 0);    // Must be positive, 0 or NaN, which should translate to baseShardCount

        // Make it a power of two multiple of the base count so that split points for lower levels remain split points for higher.
        // The conversion to int and highestOneBit round down, for which we compensate by using the sqrt(0.5) multiplier
        // already applied in targetSSTableSizeMin.
        // Setting the bottom bit to 1 ensures the result is at least baseShardCount.
        int shards = baseShardCount * Integer.highestOneBit((int) count | 1);
        logger.debug("Shard count {} for density {}, {} times target {}",
                     shards,
                     FBUtilities.prettyPrintBinary(density, "B", " "),
                     density / targetSSTableSizeMin,
                     FBUtilities.prettyPrintBinary(targetSSTableSizeMin, "B", " "));
        return shards;
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

    /**
     * The strategy will call this method each time {@link CompactionStrategy#getNextBackgroundTask(int)} is called.
     */
    public void onStrategyBackgroundTaskRequest()
    {
    }

    /**
     * Returns a maximum bucket index for the given data size and fanout.
     */
    private int maxBucketIndex(long totalLength, int fanout)
    {
        double o = getSurvivalFactor(0);
        long m = getFlushSizeBytes();
        return Math.max(0, (int) Math.floor((Math.log(totalLength) - Math.log(m)) / (Math.log(fanout) - Math.log(o))));
    }

    public static Controller fromOptions(ColumnFamilyStore cfs, Map<String, String> options)
    {
        int[] Ws = parseScalingParameters(options.getOrDefault(STATIC_SCALING_PARAMETERS_OPTION, DEFAULT_STATIC_SCALING_PARAMETERS));

        long flushSizeOverride = (long) FBUtilities.parseHumanReadable(options.getOrDefault(FLUSH_SIZE_OVERRIDE_OPTION, "0MiB"), null, "B");
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
            if (SchemaConstants.isSystemKeyspace(cfs.getKeyspaceName()) || (cfs.getDiskBoundaries().positions != null && cfs.getDiskBoundaries().positions.size() > 1))
                baseShardCount = 1;
            else
                baseShardCount = DEFAULT_BASE_SHARD_COUNT;
        }

        double targetSStableSize = options.containsKey(TARGET_SSTABLE_SIZE_OPTION)
                                   ? FBUtilities.parseHumanReadable(options.get(TARGET_SSTABLE_SIZE_OPTION), null, "B")
                                   : DEFAULT_TARGET_SSTABLE_SIZE;

        // For remote storage, the sstables on L0 are created by the different replicas, and therefore it is likely
        // that there are RF identical copies, so here we adjust the survival factor for L0
        double[] survivalFactors = System.getProperty(PREFIX + SHARED_STORAGE) == null || !Boolean.getBoolean(PREFIX + SHARED_STORAGE)
                                   ? DEFAULT_SURVIVAL_FACTORS
                                   : new double[] { DEFAULT_SURVIVAL_FACTOR / cfs.keyspace.getReplicationStrategy().getReplicationFactor().allReplicas, DEFAULT_SURVIVAL_FACTOR };

        Overlaps.InclusionMethod inclusionMethod = options.containsKey(OVERLAP_INCLUSION_METHOD_OPTION)
                                                   ? Overlaps.InclusionMethod.valueOf(options.get(OVERLAP_INCLUSION_METHOD_OPTION).toUpperCase())
                                                   : DEFAULT_OVERLAP_INCLUSION_METHOD;

        return new Controller(cfs,
                              MonotonicClock.Global.preciseTime,
                              Ws,
                              survivalFactors,
                              flushSizeOverride,
                              maxSSTablesToCompact,
                              expiredSSTableCheckFrequency,
                              ignoreOverlapsInExpirationCheck,
                              baseShardCount,
                              targetSStableSize,
                              inclusionMethod);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String nonPositiveErr = "Invalid configuration, %s should be positive: %d";
        String booleanParseErr = "%s should either be 'true' or 'false', not %s";
        String intParseErr = "%s is not a parsable int (base10) for %s";
        String longParseErr = "%s is not a parsable long (base10) for %s";
        String sizeUnacceptableErr = "%s %s is not acceptable, size must be at least %s";
        String invalidSizeErr = "%s %s is not a valid size in bytes: %s";
        options = new HashMap<>(options);
        String s;

        s = options.remove(STATIC_SCALING_PARAMETERS_OPTION);
        if (s != null)
            parseScalingParameters(s);

        s = options.remove(BASE_SHARD_COUNT_OPTION);
        if (s != null)
        {
            try
            {
                int numShards = Integer.parseInt(s);
                if (numShards <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   BASE_SHARD_COUNT_OPTION,
                                                                   numShards));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(intParseErr, s, BASE_SHARD_COUNT_OPTION), e);
            }
        }

        s = options.remove(TARGET_SSTABLE_SIZE_OPTION);
        if (s != null)
        {
            try
            {
                long targetSSTableSize = (long) FBUtilities.parseHumanReadable(s, null, "B");
                if (targetSSTableSize < MIN_TARGET_SSTABLE_SIZE)
                {
                    throw new ConfigurationException(String.format(sizeUnacceptableErr,
                                                                   TARGET_SSTABLE_SIZE_OPTION,
                                                                   s,
                                                                   FBUtilities.prettyPrintBinary(MIN_TARGET_SSTABLE_SIZE, "B", "")));
                }
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(invalidSizeErr,
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
                long flushSize = (long) FBUtilities.parseHumanReadable(s, null, "B");
                if (flushSize < MIN_TARGET_SSTABLE_SIZE)
                    throw new ConfigurationException(String.format(sizeUnacceptableErr,
                                                                   FLUSH_SIZE_OVERRIDE_OPTION,
                                                                   s,
                                                                   FBUtilities.prettyPrintBinary(MIN_TARGET_SSTABLE_SIZE, "B", "")));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(invalidSizeErr,
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
                 throw new ConfigurationException(String.format(intParseErr,
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
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION,
                                                                   expiredSSTableCheckFrequency));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(longParseErr,
                                                               s,
                                                               EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION),
                                                 e);
            }
        }

        s = options.remove(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION);
        if (s != null && !s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format(booleanParseErr,
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
