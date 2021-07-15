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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionStrategy;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.MonotonicClock;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
* The controller provides compaction parameters to the unified compaction strategy
*/
public abstract class Controller
{
    protected static final Logger logger = LoggerFactory.getLogger(Controller.class);
    private static final ConcurrentMap<TableMetadata, Controller.Metrics> allMetrics = new ConcurrentHashMap<>();

    static final String PREFIX = "unified_compaction.";

    /** The data size in GB, it will be assumed that the node will have on disk roughly this size of data when it
     * reaches equilibrium. By default 1 TB. */
    public static final String DATASET_SIZE_OPTION_GB = "dataset_size_in_gb";
    static final long DEFAULT_DATASET_SIZE_GB = Long.getLong(PREFIX + DATASET_SIZE_OPTION_GB,
                                                             DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB());

    /** The number of shards. The shard size will be calculated by dividing the data size by this number.
     * By default, 10 would be used for single disk. If the data size is 1 TB, then the shard size becomes 100 GB.
     * If JBOD / multi-drive, it would be 10 * disks. For example, if there are 5 disks, there would be 50 shards.
     * With data size 10 TB, the shard size would be 200 GB.
     * */
    static final String NUM_SHARDS_OPTION = "num_shards";
    static final int DEFAULT_NUM_SHARDS = Integer.getInteger(PREFIX + NUM_SHARDS_OPTION,
                                                             10 * DatabaseDescriptor.getAllDataFileLocations().length);

    /**
     * The minimum sstable size. Sharded writers split sstables over shard only if they are at least as large
     * as the minimum size.
     *
     * When the minimum sstable size is zero in the compaction options, then it is calculated by the controller by
     * looking at the initial flush size.
     */
    static final String MIN_SSTABLE_SIZE_OPTION_MB = "min_sstable_size_in_mb";
    static final int DEFAULT_MIN_SSTABLE_SIZE_MB = Integer.getInteger(PREFIX + MIN_SSTABLE_SIZE_OPTION_MB, 100);

    /**
     * Override for the flush size in MB. The database should be able to calculate this from executing flushes, this
     * should only be necessary in rare cases.
     */
    static final String FLUSH_SIZE_OVERRIDE_OPTION_MB = "flush_size_override_mb";

    /**
     * The maximum tolerable compaction-induced space amplification, as fraction of the dataset size. The idea behind
     * this property is to be able to tune how much to limit concurrent "oversized" compactions in different shards.
     * On one hand allowing such compactions concurrently running in all shards allows for STCS-like space
     * amplification, where at some point you might need free space double the size of your working set to do a (top
     * tier) compaction, while on the other hand limiting such compactions too much might lead to compaction lagging
     * behind, higher read amplification, and other problems of that nature.
     */
    static public final String MAX_SPACE_OVERHEAD_OPTION = "max_space_overhead";
    static final double DEFAULT_MAX_SPACE_OVERHEAD = Double.parseDouble(System.getProperty(PREFIX + MAX_SPACE_OVERHEAD_OPTION, "0.2"));
    static final double MAX_SPACE_OVERHEAD_LOWER_BOUND = 0.01;
    static final double MAX_SPACE_OVERHEAD_UPPER_BOUND = 1.0;

    /**
     * This parameter is intended to modify the shape of the LSM by taking into account the survival ratio of data, for now it is fixed to one.
     */
    static final double DEFAULT_SURVIVAL_FACTOR = Double.parseDouble(System.getProperty(PREFIX + "survival_factor", "1"));

    /**
     * Either true or false. This parameter determines which controller will be used.
     */
    static final String ADAPTIVE_OPTION = "adaptive";
    static final boolean DEFAULT_ADAPTIVE = Boolean.parseBoolean(System.getProperty(PREFIX + ADAPTIVE_OPTION, "false"));

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

    protected final MonotonicClock clock;
    protected final Environment env;
    protected final double survivalFactor;
    protected final long dataSetSizeMB;
    protected final int numShards;
    protected final long shardSizeMB;
    protected volatile long minSstableSizeMB;
    protected final double maxSpaceOverhead;
    protected final long flushSizeOverrideMB;
    protected volatile long currentFlushSize;
    protected final int maxSSTablesToCompact;
    protected final long expiredSSTableCheckFrequency;
    protected final boolean ignoreOverlapsInExpirationCheck;
    @Nullable protected volatile CostsCalculator calculator;
    @Nullable private volatile Metrics metrics;

    Controller(MonotonicClock clock,
               Environment env,
               double survivalFactor,
               long dataSetSizeMB,
               int numShards,
               long minSstableSizeMB,
               long flushSizeOverrideMB,
               double maxSpaceOverhead,
               int maxSSTablesToCompact,
               long expiredSSTableCheckFrequency,
               boolean ignoreOverlapsInExpirationCheck)
    {
        this.clock = clock;
        this.env = env;
        this.survivalFactor = survivalFactor;
        this.dataSetSizeMB = dataSetSizeMB;
        this.numShards = numShards;
        this.shardSizeMB = (int) Math.ceil((double) dataSetSizeMB / numShards);
        this.minSstableSizeMB = minSstableSizeMB;
        this.flushSizeOverrideMB = flushSizeOverrideMB;
        this.currentFlushSize = flushSizeOverrideMB << 20;
        this.expiredSSTableCheckFrequency = TimeUnit.MILLISECONDS.convert(expiredSSTableCheckFrequency, TimeUnit.SECONDS);

        double maxSpaceOverheadLowerBound = 1.0d / numShards;
        if (maxSpaceOverhead < maxSpaceOverheadLowerBound)
        {
            logger.warn("{} shards are not enough to maintain the required maximum space overhead of {}!\n" +
                        "Falling back to {}={} instead. If this limit needs to be satisfied, please increase the number" +
                        " of shards.",
                        numShards,
                        maxSpaceOverhead,
                        MAX_SPACE_OVERHEAD_OPTION,
                        String.format("%.3f", maxSpaceOverheadLowerBound));
            this.maxSpaceOverhead = maxSpaceOverheadLowerBound;
        }
        else
            this.maxSpaceOverhead = maxSpaceOverhead;

        if (maxSSTablesToCompact <= 0)  // use half the maximum permitted compaction size as upper bound by default
            maxSSTablesToCompact = (int) (dataSetSizeMB * this.maxSpaceOverhead * 0.5 / minSstableSizeMB);

        this.maxSSTablesToCompact = maxSSTablesToCompact;

        if (ignoreOverlapsInExpirationCheck && !ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION)
        {
            logger.warn("Not enabling aggressive SSTable expiration, as the system property '" + ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_PROPERTY + "' is set to 'false'. " +
                    "Set it to 'true' to enable aggressive SSTable expiration.");
        }
        this.ignoreOverlapsInExpirationCheck = ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION && ignoreOverlapsInExpirationCheck;
    }

    @VisibleForTesting
    public Environment getEnv()
    {
        return env;
    }

    /**
     * @return the scaling parameter W
     * @param index
     */
    public abstract int getScalingParameter(int index);

    public int getFanout(int index) {
        int W = getScalingParameter(index);
        return W < 0 ? 2 - W : 2 + W; // see formula in design doc
    }

    public int getThreshold(int index) {
        int W = getScalingParameter(index);
        return W < 0 ? 2 : getFanout(index); // see formula in design doc
    }

    /**
     * @return the number of shards according to the dataset and shard sizes set by the user
     */
    public int getNumShards()
    {
        return numShards;
    }

    /**
     * @return the survival factor o
     */
    public double getSurvivalFactor()
    {
        return survivalFactor;
    }

    /**
     * The user specified dataset size.
     *
     * @return the target size of the entire data set, in bytes.
     */
    public long getDataSetSizeBytes()
    {
        return dataSetSizeMB << 20;
    }

    /**
     * The user specified shard, or compaction arena, size.
     *
     * @return the desired size of each shard, or compaction arena, in bytes.
     */
    public long getShardSizeBytes()
    {
        return shardSizeMB << 20;
    }

    /**
     * Return the sstable size in bytes.
     *
     * This is either set by the user in the options or calculated by rounding up the first flush size to 50 MB.
     *
     * @return the minimum sstable size in bytes.
     */
    public long getMinSstableSizeBytes()
    {
        if (minSstableSizeMB > 0)
            return minSstableSizeMB << 20;

        synchronized (this)
        {
            if (minSstableSizeMB > 0)
                return minSstableSizeMB << 20;

            // round the avg flush size to the nearest byte
            long envFlushSize = Math.round(env.flushSize());
            long fiftyMB = 50 << 20;

            // round up to 50 MB
            long flushSize = ((Math.max(1, envFlushSize) + fiftyMB - 1) / fiftyMB) * fiftyMB;

            // If the env flush size is positive, then we've flushed at least once and we use this value permanently
            if (envFlushSize > 0)
                minSstableSizeMB = flushSize >> 20;

            return flushSize;
        }
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
        if (flushSizeOverrideMB > 0)
            return flushSizeOverrideMB << 20;

        double envFlushSize = env.flushSize();
        if (currentFlushSize == 0 || Math.abs(1 - (currentFlushSize / envFlushSize)) > 0.5)
        {
            // The current size is not initialized, or it differs by over 50% from the observed.
            // Use the observed size rounded up to a whole megabyte.
            currentFlushSize = ((long) (Math.ceil(Math.scalb(envFlushSize, -20)))) << 20;
        }
        return currentFlushSize;
    }

    /**
     * Returns the maximum tolerable compaction-induced space amplification, as a fraction of the dataset size.
     * Currently this is not a strict limit for which compaction gives an ironclad guarantee never to exceed it, but
     * the main input in a simple heuristic that is designed to limit UCS' space amplification in exchange of some
     * delay in top bucket compactions.
     *
     * @return a {@code double} value between 0.01 and 1.0, representing the fraction of the expected uncompacted
     * dataset size that should be additionally available for compaction's space amplification overhead.
     */
    public double getMaxSpaceOverhead()
    {
        return maxSpaceOverhead;
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
     * Perform any initialization that requires the strategy.
     */
    public void startup(UnifiedCompactionStrategy strategy, ScheduledExecutorService executorService)
    {
        if (calculator != null)
            throw new IllegalStateException("Already started");

        startup(strategy, new CostsCalculator(env, strategy, executorService, survivalFactor));
    }

    @VisibleForTesting
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        this.calculator = calculator;
        metrics = allMetrics.computeIfAbsent(strategy.getMetadata(), Controller.Metrics::new);
        metrics.setController(this);
        logger.debug("Started compaction controller {}", this);
    }

    /**
     * Signals that the strategy is about to be deleted or stopped.
     */
    public void shutdown()
    {
        if (calculator == null)
            return;

        calculator.close();
        calculator = null;

        if (metrics != null)
        {
            metrics.release();
            metrics.removeController();
            metrics = null;
        }

        logger.debug("Stopped compaction controller {}", this);
    }

    /**
     * @return true if the controller is running
     */
    public boolean isRunning()
    {
        return calculator != null;
    }

    /**
     * @return the cost calculator, will be null until {@link this#startup(UnifiedCompactionStrategy, ScheduledExecutorService)} is called.
     */
    @Nullable
    @VisibleForTesting
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    /**
     * The strategy will call this method each time {@link CompactionStrategy#getNextBackgroundTasks(int)} is called.
     */
    public void onStrategyBackgroundTaskRequest()
    {
    }

    /**
     * Calculate the read amplification assuming a single scaling parameter W and a given total
     * length of data on disk.
     *
     * @param length the total length on disk
     * @param scalingParameter the scaling parameter to use for the calculation
     *
     * @return the read amplification of all the buckets needed to cover the total length
     */
    public int readAmplification(long length, int scalingParameter)
    {
        double o = getSurvivalFactor();
        long m = getFlushSizeBytes();

        int F = scalingParameter < 0 ? 2 - scalingParameter : 2 + scalingParameter;
        int T = scalingParameter < 0 ? 2 : F;
        int maxIndex = maxBucketIndex(length, F);

        int ret = 0;
        for (int i = 0; i < maxIndex; i++)
            ret += T - 1;

        if (scalingParameter >= 0)
            ret += Math.max(0, Math.ceil(length / (m * Math.pow(o * F, maxIndex))) - 1);
        else
            ret += 1;

        return ret;
    }

    /**
     * Calculate the write amplification assuming a single scaling parameter W and a given total
     * length of data on disk.
     *
     * @param length the total length on disk
     * @param scalingParameter the scaling parameter to use for the calculation
     *
     * @return the write amplification of all the buckets needed to cover the total length
     */
    public int writeAmplification(long length, int scalingParameter)
    {
        double o = getSurvivalFactor();
        long m = getFlushSizeBytes();

        int F = scalingParameter < 0 ? 2 - scalingParameter : 2 + scalingParameter;
        int maxIndex = maxBucketIndex(length, F);

        int ret = 0;

        if (scalingParameter >= 0)
        {   // for tiered, at each level the WA is 1. We start at level 0 and end up at level maxIndex so that's a WA of maxIndex.
            ret += maxIndex + 1;
        }
        else
        {   // for leveled, at each level the WA is F - 1 except for the last one, where it's (size / size of previous level) - 1
            // or (size / (m*(o*F)^maxIndex)) - 1
            for (int i = 0; i < maxIndex; i++)
                ret += F - 1;

            ret += Math.max(0, Math.ceil(length / (m * Math.pow(o * F, maxIndex))));
        }

        return ret;
    }

    /**
     * Returns a maximum bucket index for the given data size and fanout.
     */
    private int maxBucketIndex(long totalLength, int fanout)
    {
        double o = getSurvivalFactor();
        long m = getFlushSizeBytes();
        return Math.max(0, (int) Math.floor((Math.log(totalLength) - Math.log(m)) / (Math.log(fanout) - Math.log(o))));
    }

    private double getReadIOCost()
    {
        if (calculator == null)
            return 0;

        int W = getScalingParameter(0);
        long length = (long) Math.ceil(calculator.spaceUsed());
        return calculator.getReadCostForQueries(readAmplification(length, W));
    }

    private double getWriteIOCost()
    {
        if (calculator == null)
            return 0;

        int W = getScalingParameter(0);
        long length = (long) Math.ceil(calculator.spaceUsed());
        return calculator.getWriteCostForQueries(writeAmplification(length, W));
    }

    public static Controller fromOptions(ColumnFamilyStore cfs, Map<String, String> options)
    {
        boolean adaptive = options.containsKey(ADAPTIVE_OPTION) ? Boolean.parseBoolean(options.get(ADAPTIVE_OPTION)) : DEFAULT_ADAPTIVE;
        long dataSetSizeMb = (options.containsKey(DATASET_SIZE_OPTION_GB) ? Long.parseLong(options.get(DATASET_SIZE_OPTION_GB)) : DEFAULT_DATASET_SIZE_GB) << 10;
        int numShards = options.containsKey(NUM_SHARDS_OPTION) ? Integer.parseInt(options.get(NUM_SHARDS_OPTION)) : DEFAULT_NUM_SHARDS;
        long sstableSizeMb = options.containsKey(MIN_SSTABLE_SIZE_OPTION_MB) ? Long.parseLong(options.get(MIN_SSTABLE_SIZE_OPTION_MB)) : DEFAULT_MIN_SSTABLE_SIZE_MB;
        long flushSizeOverrideMb = Long.parseLong(options.getOrDefault(FLUSH_SIZE_OVERRIDE_OPTION_MB, "0"));
        double maxSpaceOverhead = options.containsKey(MAX_SPACE_OVERHEAD_OPTION)
                ? Double.parseDouble(options.get(MAX_SPACE_OVERHEAD_OPTION))
                : DEFAULT_MAX_SPACE_OVERHEAD;
        int maxSSTablesToCompact = Integer.parseInt(options.getOrDefault(MAX_SSTABLES_TO_COMPACT_OPTION, "0"));
        long expiredSSTableCheckFrequency = options.containsKey(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION)
                ? Long.parseLong(options.get(EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS_OPTION))
                : DEFAULT_EXPIRED_SSTABLE_CHECK_FREQUENCY_SECONDS;
        boolean ignoreOverlapsInExpirationCheck = options.containsKey(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION)
                ? Boolean.parseBoolean(options.get(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION))
                : DEFAULT_ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION;

        Environment env = new RealEnvironment(cfs);

        return adaptive
               ? AdaptiveController.fromOptions(env,
                                                DEFAULT_SURVIVAL_FACTOR,
                                                dataSetSizeMb,
                                                numShards,
                                                sstableSizeMb,
                                                flushSizeOverrideMb,
                                                maxSpaceOverhead,
                                                maxSSTablesToCompact,
                                                expiredSSTableCheckFrequency,
                                                ignoreOverlapsInExpirationCheck,
                                                options)
               : StaticController.fromOptions(env,
                                              DEFAULT_SURVIVAL_FACTOR,
                                              dataSetSizeMb,
                                              numShards,
                                              sstableSizeMb,
                                              flushSizeOverrideMb,
                                              maxSpaceOverhead,
                                              maxSSTablesToCompact,
                                              expiredSSTableCheckFrequency,
                                              ignoreOverlapsInExpirationCheck,
                                              options);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String nonPositiveErr = "Invalid configuration, %s should be positive: %d";
        String booleanParseErr = "%s should either be 'true' or 'false', not %s";
        String intParseErr = "%s is not a parsable int (base10) for %s";
        String longParseErr = "%s is not a parsable long (base10) for %s";
        String floatParseErr = "%s is not a parsable float for %s";
        options = new HashMap<>(options);
        String s;
        boolean adaptive = DEFAULT_ADAPTIVE;

        s = options.remove(ADAPTIVE_OPTION);
        if (s != null)
        {
            if (!s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
            {
                throw new ConfigurationException(String.format(booleanParseErr, ADAPTIVE_OPTION, s));
            }
            adaptive = Boolean.parseBoolean(s);
        }

        s = options.remove(MIN_SSTABLE_SIZE_OPTION_MB);
        if (s != null)
        {
            try
            {
                long minSStableSize = Long.parseLong(s);
                if (minSStableSize <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   MIN_SSTABLE_SIZE_OPTION_MB,
                                                                   minSStableSize));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(longParseErr,
                                                               s,
                                                               MIN_SSTABLE_SIZE_OPTION_MB),
                                                 e);
            }
        }

        s = options.remove(FLUSH_SIZE_OVERRIDE_OPTION_MB);
        if (s != null)
        {
            try
            {
                long flushSize = Long.parseLong(s);
                if (flushSize <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   FLUSH_SIZE_OVERRIDE_OPTION_MB,
                                                                   flushSize));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(longParseErr,
                                                               s,
                                                               FLUSH_SIZE_OVERRIDE_OPTION_MB),
                                                 e);
            }
        }

        s = options.remove(DATASET_SIZE_OPTION_GB);
        if (s != null)
        {
            try
            {
                long dataSetSizeMb = Long.parseLong(s);
                if (dataSetSizeMb <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   DATASET_SIZE_OPTION_GB,
                                                                   dataSetSizeMb));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(longParseErr, s, DATASET_SIZE_OPTION_GB), e);
            }
        }

        s = options.remove(NUM_SHARDS_OPTION);
        if (s != null)
        {
            try
            {
                int numShards = Integer.parseInt(s);
                if (numShards <= 0)
                    throw new ConfigurationException(String.format(nonPositiveErr,
                                                                   NUM_SHARDS_OPTION,
                                                                   numShards));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(intParseErr, s, NUM_SHARDS_OPTION), e);
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

        s = options.remove(MAX_SPACE_OVERHEAD_OPTION);
        if (s != null)
        {
            try
            {
                double maxSpaceOverhead = Double.parseDouble(s);
                if (maxSpaceOverhead < MAX_SPACE_OVERHEAD_LOWER_BOUND || maxSpaceOverhead > MAX_SPACE_OVERHEAD_UPPER_BOUND)
                    throw new ConfigurationException(String.format("Invalid configuration, %s must be between %f and %f: %s",
                                                                   MAX_SPACE_OVERHEAD_OPTION,
                                                                   MAX_SPACE_OVERHEAD_LOWER_BOUND,
                                                                   MAX_SPACE_OVERHEAD_UPPER_BOUND,
                                                                   s));
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format(floatParseErr,
                                                               s,
                                                               MAX_SPACE_OVERHEAD_OPTION),
                                                 e);
            }
        }

        s = options.remove(ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION);
        if (s != null && !s.equalsIgnoreCase("true") && !s.equalsIgnoreCase("false"))
        {
            throw new ConfigurationException(String.format(booleanParseErr,
                                                           ALLOW_UNSAFE_AGGRESSIVE_SSTABLE_EXPIRATION_OPTION, s));
        }

        return adaptive ? AdaptiveController.validateOptions(options) : StaticController.validateOptions(options);
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
        return Math.max(1 << 20, getFlushSizeBytes()) * (1.0 - 0.9 / F) / getNumShards();
    }

    public double maxThroughput()
    {
        final int compactionThroughputMbPerSec = DatabaseDescriptor.getCompactionThroughputMbPerSec();
        if (compactionThroughputMbPerSec <= 0)
            return Double.MAX_VALUE;
        return compactionThroughputMbPerSec * 1024.0 * 1024.0;
    }

    public int maxConcurrentCompactions()
    {
        return DatabaseDescriptor.getConcurrentCompactors();
    }

    public long maxCompactionSpaceBytes()
    {
        // Note: Compaction will not proceed with operations larger than this size (i.e. it will compact on the lower
        // levels but will accumulate sstables on the top until the space on the drive fills up). This sounds risky but
        // is less of a problem than running out of space during compaction.
        return (long) (getDataSetSizeBytes() * getMaxSpaceOverhead());
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

    static final class Metrics
    {
        private final MetricNameFactory factory;
        private final AtomicReference<Controller> controllerRef;
        private final Gauge<Double> totWAGauge;
        private final Gauge<Double> readIOCostGauge;
        private final Gauge<Double> writeIOCostGauge;
        private final Gauge<Double> totIOCostGauge;

        Metrics(TableMetadata metadata)
        {
            this.factory = new DefaultNameFactory("CompactionCosts",
                                                  String.format("%s.%s", metadata.keyspace, metadata.name));
            this.controllerRef = new AtomicReference<>();
            this.totWAGauge = Metrics.register(factory.createMetricName("WA"), this::getMeasuredWA);
            this.readIOCostGauge = Metrics.register(factory.createMetricName("ReadIOCost"), this::getReadIOCost);
            this.writeIOCostGauge = Metrics.register(factory.createMetricName("WriteIOCost"), this::getWriteIOCost);
            this.totIOCostGauge = Metrics.register(factory.createMetricName("TotIOCost"), this::getTotalIOCost);
        }

        void setController(Controller controller)
        {
            this.controllerRef.set(controller);
        }

        void removeController()
        {
           this.controllerRef.set(null);
        }

        void release()
        {
            Metrics.remove(factory.createMetricName("WA"));
            Metrics.remove(factory.createMetricName("ReadIOCost"));
            Metrics.remove(factory.createMetricName("WriteIOCost"));
            Metrics.remove(factory.createMetricName("TotIOCost"));
        }

        double getMeasuredWA()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.env.WA();

            return ret;
        }

        double getReadIOCost()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.getReadIOCost();

            return ret;
        }

        double getWriteIOCost()
        {
            double ret = 0;
            Controller controller = controllerRef.get();
            if (controller != null)
                ret = controller.getWriteIOCost();

            return ret;
        }

        double getTotalIOCost()
        {
            return getReadIOCost() + getWriteIOCost();
        }
    }
}