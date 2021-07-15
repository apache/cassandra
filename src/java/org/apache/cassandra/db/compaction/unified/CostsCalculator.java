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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CompactionMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MovingAverage;

/**
 * This class periodically retrieves delta values from the environment and stores them into exponentially weighted averages.
 * It then uses these values to calculate IO costs that are exported to {@link CompactionMetrics} and used by {@link AdaptiveController}
 * to choose the optimal configuration for compaction.
 */
public class CostsCalculator
{
    private final static Logger logger = LoggerFactory.getLogger(CostsCalculator.class);

    /** How often values are sampled. Sampling for periods that are too short (<= 1 second) may not give good results since
     * we many not collect sufficient data. */
    final static int samplingPeriodMs = Integer.getInteger(Controller.PREFIX + "sample_time_ms", 5000);

    /** The multipliers can be used by users if they wish to adjust the costs. We reduce the read costs because writes are batch processes (flush and compaction)
     * and therefore the costs tend to be lower that for reads, so by reducing read costs we make the costs more comparable.
     */
    final static double defaultWriteMultiplier = Double.parseDouble(System.getProperty(Controller.PREFIX + "costs_write_multiplier", "1"));
    final static double defaultReadMultiplier = Double.parseDouble(System.getProperty(Controller.PREFIX + "costs_read_multiplier", "0.1"));

    private final Environment env;
    private final double readMultiplier;
    private final double writeMultiplier;
    private final double survivalFactor;
    private final MovingAverageOfDelta partitionsReadPerPeriod;
    private final MovingAverageOfDelta bytesInsertedPerPeriod;
    private final MovingAverage numSSTables;
    private final MovingAverage spaceUsed;
    private final UnifiedCompactionStrategy strategy;

    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock.ReadLock readLock;
    private final ReentrantReadWriteLock.WriteLock writeLock;
    private final ScheduledFuture<?> future;

    CostsCalculator(Environment env,
                    UnifiedCompactionStrategy strategy,
                    ScheduledExecutorService executorService,
                    double survivalFactor)
    {
        this(env, strategy, executorService, survivalFactor, defaultReadMultiplier, defaultWriteMultiplier);
    }

    CostsCalculator(Environment env,
                    UnifiedCompactionStrategy strategy,
                    ScheduledExecutorService executorService,
                    double survivalFactor,
                    double readMultiplier,
                    double writeMultiplier)
    {
        this.env = env;
        this.readMultiplier = readMultiplier;
        this.writeMultiplier = writeMultiplier;
        this.survivalFactor = survivalFactor;
        this.partitionsReadPerPeriod = new MovingAverageOfDelta(env.makeExpMovAverage());
        this.bytesInsertedPerPeriod = new MovingAverageOfDelta(env.makeExpMovAverage());
        this.numSSTables = env.makeExpMovAverage();
        this.spaceUsed = env.makeExpMovAverage();
        this.strategy = strategy;
        this.lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();
        this.future = executorService.scheduleAtFixedRate(this::sampleValues, samplingPeriodMs, samplingPeriodMs, TimeUnit.MILLISECONDS);
    }

    public void close()
    {
        writeLock.lock();

        try
        {
            logger.debug("Stopping cost calculations for {}", strategy.getMetadata());
            future.cancel(false);
            logger.debug("Stopped cost calculations for {}", strategy.getMetadata());
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @VisibleForTesting
    void sampleValues()
    {
        writeLock.lock();

        try
        {
            partitionsReadPerPeriod.update(env.partitionsRead());
            bytesInsertedPerPeriod.update(env.bytesInserted());

            numSSTables.update(strategy.getSSTables().size());
            spaceUsed.update(strategy.getSSTables().stream().map(SSTableReader::onDiskLength).reduce(0L, Long::sum));
        }
        catch (Throwable err)
        {
            JVMStabilityInspector.inspectThrowable(err);
            logger.error("Failed to update values: {}/{}", err.getClass().getName(), err.getMessage(), err);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    /**
     * @return the estimated read cost for the given number of partitions, in milliseconds
     */
    private double getReadCost(double partitionsRead)
    {
        return (env.sstablePartitionReadLatencyNanos() * partitionsRead) / TimeUnit.MILLISECONDS.toNanos(1);
    }

    /**
     * Calculate the projected read cost for user queries.
     *
     * The projected read cost is given by the number of partitions read, times the mean partition latency and is calculated
     * by {@link this#getReadCost(double)}. This value is then multiplied by the number of sstables we're likely to hit
     * per partition read and the read multiplier.
     * <p/>
     * The number of sstables is calculated as Math.min(1 + env.bloomFilterFpRatio() * RA / survivalFactor, RA). Here we
     * assume there is going to be at least one sstable accessed, possibly more in case of :
     *
     * - bloom filter's false positives;
     * - partitions not surviving a compaction (1/survivalFactor is the limit of the sum of (1-survivalFactor)^n), that
     *   is partitions that would not exist if compaction was done; Note that the survival factor is currently fixed to 1.
     *
     * The RA is then a cap since we cannot read more than RA sstables, which are the sstables that exist because
     * compactions allows them to exist.
     * </p>
     * The read multiplier is a factor that operators can use to tweak the algorithm.
     * </p>
     * @param RA the expected read amplification due to the current choice of compaction strategy
     *
     * @return the projected read cost for user queries
     */
    public double getReadCostForQueries(int RA)
    {
        readLock.lock();

        try
        {
            return getReadCost(partitionsReadPerPeriod.avg.get()) * Math.min(1 + env.bloomFilterFpRatio() * RA / survivalFactor, RA) * readMultiplier;
        }
        finally
        {
            readLock.unlock();
        }
    }

    private double getFlushCost(double bytesWritten)
    {
        return ((bytesWritten / (1 << 10)) * env.flushLatencyPerKbInNanos()) / (double) TimeUnit.MILLISECONDS.toNanos(1);
    }

    private double getCompactionCost(double bytesWritten)
    {
        // So, the compaction latency will depend on the size of the sstables, so in the correct solution each level
        // should pass its output size and we should measure latency in MB or something like that
        return ((bytesWritten / (1 << 10)) * env.compactionLatencyPerKbInNanos()) / (double)  TimeUnit.MILLISECONDS.toNanos(1);
    }

    /**
     * Calculate the projected write cost for user insertions.
     *
     * The projected write cost is given by the number of bytes that were inserted times the flush cost
     * plus the same number of bytes times the compaction cost and the compaction WA. We also multiply by
     * a write multiplier to let users change the weights if needed.
     *
     * @param WA the expected write amplification due to compaction
     *
     * @return the projected flush and write cost.
     */
    public double getWriteCostForQueries(int WA)
    {
        readLock.lock();

        try
        {
            double bytesInserted = this.bytesInsertedPerPeriod.avg.get();
            // using bytesInserted for the compaction cost doesn't take into account overwrites but for now it's good enough
            return (getFlushCost(bytesInserted) + getCompactionCost(bytesInserted) * WA) * writeMultiplier;
        }
        finally
        {
            readLock.unlock();
        }
    }

    public double partitionsRead()
    {
        return partitionsReadPerPeriod.avg.get();
    }

    public double numSSTables()
    {
        return numSSTables.get();
    }

    public double spaceUsed()
    {
        return spaceUsed.get();
    }

    public Environment getEnv()
    {
        return env;
    }

    @Override
    public String toString()
    {
        return String.format("num partitions read %s, bytes inserted: %s, num sstables %s; Environment: %s",
                             partitionsReadPerPeriod, bytesInsertedPerPeriod, numSSTables, env);
    }

    @NotThreadSafe
    private static final class MovingAverageOfDelta
    {
        private final MovingAverage avg;
        private volatile double prev;

        MovingAverageOfDelta(MovingAverage avg)
        {
            this.avg = avg;
            this.prev = Double.MIN_VALUE;
        }

        void update(double val)
        {
            if (prev != Double.MIN_VALUE)
                avg.update(val - prev);

            prev = val;
        }

        @Override
        public String toString()
        {
            return String.format("%s/%d sec", FBUtilities.prettyPrintMemory((long) avg.get()), TimeUnit.MILLISECONDS.toSeconds(samplingPeriodMs));
        }
    }
}