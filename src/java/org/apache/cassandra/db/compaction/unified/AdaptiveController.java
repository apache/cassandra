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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.CompactionPick;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.Overlaps;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * The adaptive compaction controller dynamically calculates the optimal scaling parameter W.
 * <p/>
 * Generally it tries to find a local minimum for the total IO cost that is projected
 * by the strategy. The projected IO cost is composed by two parts: the read amplification,
 * which is weighted by the number of partitions read by the user, and the write amplification, which
 * is weighted by the number of bytes inserted into memtables. Other parameters are also considered, such
 * as the cache miss rate and the time it takes to read and write from disk. See also the comments in
 * {@link CostsCalculator}.
 *
 * Design doc: TODO: link to design doc or SEP
 */
public class AdaptiveController extends Controller
{
    private static final Logger logger = LoggerFactory.getLogger(AdaptiveController.class);

    /** The starting value for the scaling parameter */
    private static final int DEFAULT_STARTING_SCALING_PARAMETER = 0;

    /** The minimum valid value for the scaling parameter */
    static final String MIN_SCALING_PARAMETER = "adaptive_min_scaling_parameter";
    static private final int DEFAULT_MIN_SCALING_PARAMETER = Integer.getInteger(PREFIX + MIN_SCALING_PARAMETER, -10);

    /** The maximum valid value for the scaling parameter */
    static final String MAX_SCALING_PARAMETER = "adaptive_max_scaling_parameter";
    static private final int DEFAULT_MAX_SCALING_PARAMETER = Integer.getInteger(PREFIX + MAX_SCALING_PARAMETER, 36);

    /** The interval for periodically checking the optimal value for the scaling parameter */
    static final String INTERVAL_SEC = "adaptive_interval_sec";
    static private final int DEFAULT_INTERVAL_SEC = Integer.getInteger(PREFIX + INTERVAL_SEC, 300);

    /** The gain is a number between 0 and 1 used to determine if a new choice of the scaling parameter is better than the current one */
    static final String THRESHOLD = "adaptive_threshold";
    private static final double DEFAULT_THRESHOLD = Double.parseDouble(System.getProperty(PREFIX + THRESHOLD, "0.15"));

    /** Below the minimum cost we don't try to optimize the scaling parameter, we consider the current scaling parameter good enough. This is necessary because the cost
     * can vanish to zero when there are neither reads nor writes and right now we don't know how to handle this case.  */
    static final String MIN_COST = "adaptive_min_cost";
    static private final int DEFAULT_MIN_COST = Integer.getInteger(PREFIX + MIN_COST, 1000);

    /** The maximum number of concurrent Adaptive Compactions */
    static final String MAX_ADAPTIVE_COMPACTIONS = "max_adaptive_compactions";
    private static final int DEFAULT_MAX_ADAPTIVE_COMPACTIONS = Integer.getInteger(PREFIX + MAX_ADAPTIVE_COMPACTIONS, 5);
    private final int intervalSec;
    private final int minScalingParameter;
    private final int maxScalingParameter;
    private final double threshold;
    private final int minCost;
    /** Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks */
    private int[] scalingParameters;
    private int[] previousScalingParameters;
    private volatile long lastChecked;
    private final int maxAdaptiveCompactions;

    @VisibleForTesting
    public AdaptiveController(MonotonicClock clock,
                              Environment env,
                              int[] scalingParameters,
                              int[] previousScalingParameters,
                              double[] survivalFactors,
                              long dataSetSize,
                              long minSSTableSize,
                              long flushSizeOverride,
                              long currentFlushSize,
                              double maxSpaceOverhead,
                              int maxSSTablesToCompact,
                              long expiredSSTableCheckFrequency,
                              boolean ignoreOverlapsInExpirationCheck,
                              int baseShardCount,
                              long targetSStableSize,
                              double sstableGrowthModifier,
                              int reservedThreadsPerLevel,
                              Reservations.Type reservationsType,
                              Overlaps.InclusionMethod overlapInclusionMethod,
                              int intervalSec,
                              int minScalingParameter,
                              int maxScalingParameter,
                              double threshold,
                              int minCost,
                              int maxAdaptiveCompactions,
                              String keyspaceName,
                              String tableName)
    {
        super(clock,
              env,
              survivalFactors,
              dataSetSize,
              minSSTableSize,
              flushSizeOverride,
              currentFlushSize,
              maxSpaceOverhead,
              maxSSTablesToCompact,
              expiredSSTableCheckFrequency,
              ignoreOverlapsInExpirationCheck,
              baseShardCount,
              targetSStableSize,
              sstableGrowthModifier,
              reservedThreadsPerLevel,
              reservationsType,
              overlapInclusionMethod);

        this.scalingParameters = scalingParameters;
        this.previousScalingParameters = previousScalingParameters;
        this.intervalSec = intervalSec;
        this.minScalingParameter = minScalingParameter;
        this.maxScalingParameter = maxScalingParameter;
        this.threshold = threshold;
        this.minCost = minCost;
        this.maxAdaptiveCompactions = maxAdaptiveCompactions;
        this.keyspaceName = keyspaceName;
        this.tableName = tableName;
    }

    static Controller fromOptions(Environment env,
                                  double[] survivalFactors,
                                  long dataSetSize,
                                  long minSSTableSize,
                                  long flushSizeOverride,
                                  double maxSpaceOverhead,
                                  int maxSSTablesToCompact,
                                  long expiredSSTableCheckFrequency,
                                  boolean ignoreOverlapsInExpirationCheck,
                                  int baseShardCount,
                                  long targetSSTableSize,
                                  double sstableGrowthModifier,
                                  int reservedThreadsPerLevel,
                                  Reservations.Type reservationsType,
                                  Overlaps.InclusionMethod overlapInclusionMethod,
                                  String keyspaceName,
                                  String tableName,
                                  Map<String, String> options)
    {
        int[] scalingParameters = null;
        long currentFlushSize = flushSizeOverride;

        File f = getControllerConfigPath(keyspaceName, tableName);
        try
        {
            JSONParser jsonParser = new JSONParser();
            JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(f));
            scalingParameters = readStoredScalingParameters((JSONArray) jsonObject.get("scaling_parameters"));
            if (jsonObject.get("current_flush_size") != null && flushSizeOverride == 0)
            {
                currentFlushSize = (long) jsonObject.get("current_flush_size");
                logger.debug("Successfully read stored current_flush_size from disk");
            }
        }
        catch (IOException e)
        {
            logger.debug("No controller config file found. Using starting value instead.");
        }
        catch (ParseException e)
        {
            logger.warn("Unable to parse saved options. Using starting value instead:", e);
        }

        if (scalingParameters == null)
        {
            logger.info("Unable to read scaling_parameters. Using starting value instead.");
            scalingParameters = new int[UnifiedCompactionStrategy.MAX_LEVELS];
            String staticScalingParameters = options.remove(SCALING_PARAMETERS_OPTION);
            String staticScalingFactors = options.remove(STATIC_SCALING_FACTORS_OPTION);

            if (staticScalingParameters != null)
            {
                int[] parameters = parseScalingParameters(staticScalingParameters);
                for (int i = 0; i < scalingParameters.length; i++)
                {
                    if (i < parameters.length)
                        scalingParameters[i] = parameters[i];
                    else
                        scalingParameters[i] = scalingParameters[i-1];
                }
            }
            else if (staticScalingFactors != null)
            {
                int[] factors = parseScalingParameters(staticScalingFactors);
                for (int i = 0; i < scalingParameters.length; i++)
                {
                    if (i < factors.length)
                        scalingParameters[i] = factors[i];
                    else
                        scalingParameters[i] = scalingParameters[i-1];
                }
                logger.info("Option: '{}' used to initialize scaling parameters for Adaptive Controller", STATIC_SCALING_FACTORS_OPTION);
            }
            else
                Arrays.fill(scalingParameters, DEFAULT_STARTING_SCALING_PARAMETER);
        }
        else
        {
            logger.debug("Successfully read stored scaling parameters from disk.");
            if (options.containsKey(SCALING_PARAMETERS_OPTION))
                logger.warn("Option: '{}' is defined but not used.  Stored configuration was used instead", SCALING_PARAMETERS_OPTION);
            if (options.containsKey(STATIC_SCALING_FACTORS_OPTION))
                logger.warn("Option: '{}' is defined but not used.  Stored configuration was used instead", STATIC_SCALING_FACTORS_OPTION);
        }
        int[] previousScalingParameters = scalingParameters.clone();

        int minScalingParameter = options.containsKey(MIN_SCALING_PARAMETER) ? Integer.parseInt(options.get(MIN_SCALING_PARAMETER)) : DEFAULT_MIN_SCALING_PARAMETER;
        int maxScalingParameter = options.containsKey(MAX_SCALING_PARAMETER) ? Integer.parseInt(options.get(MAX_SCALING_PARAMETER)) : DEFAULT_MAX_SCALING_PARAMETER;
        int intervalSec = options.containsKey(INTERVAL_SEC) ? Integer.parseInt(options.get(INTERVAL_SEC)) : DEFAULT_INTERVAL_SEC;
        double threshold = options.containsKey(THRESHOLD) ? Double.parseDouble(options.get(THRESHOLD)) : DEFAULT_THRESHOLD;
        int minCost = options.containsKey(MIN_COST) ? Integer.parseInt(options.get(MIN_COST)) : DEFAULT_MIN_COST;
        int maxAdaptiveCompactions = options.containsKey(MAX_ADAPTIVE_COMPACTIONS) ? Integer.parseInt(options.get(MAX_ADAPTIVE_COMPACTIONS)) : DEFAULT_MAX_ADAPTIVE_COMPACTIONS;

        return new AdaptiveController(MonotonicClock.preciseTime,
                                      env,
                                      scalingParameters,
                                      previousScalingParameters,
                                      survivalFactors,
                                      dataSetSize,
                                      minSSTableSize,
                                      flushSizeOverride,
                                      currentFlushSize,
                                      maxSpaceOverhead,
                                      maxSSTablesToCompact,
                                      expiredSSTableCheckFrequency,
                                      ignoreOverlapsInExpirationCheck,
                                      baseShardCount,
                                      targetSSTableSize,
                                      sstableGrowthModifier,
                                      reservedThreadsPerLevel,
                                      reservationsType,
                                      overlapInclusionMethod,
                                      intervalSec,
                                      minScalingParameter,
                                      maxScalingParameter,
                                      threshold,
                                      minCost,
                                      maxAdaptiveCompactions,
                                      keyspaceName,
                                      tableName);
    }

    private static int[] readStoredScalingParameters(JSONArray storedScalingParameters)
    {
        if (storedScalingParameters.size() > 0)
        {
            int[] scalingParameters = new int[UnifiedCompactionStrategy.MAX_LEVELS];
            for (int i = 0; i < scalingParameters.length; i++)
            {
                //if the file does not have enough entries, use the last entry for the rest of the levels
                if (i < storedScalingParameters.size())
                    scalingParameters[i] = ((Long) storedScalingParameters.get(i)).intValue();
                else
                    scalingParameters[i] = scalingParameters[i-1];
            }
            //successfuly read scaling_parameters
            return scalingParameters;
        }
        else
        {
            return null;
        }
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        int scalingParameter = DEFAULT_STARTING_SCALING_PARAMETER;
        int minScalingParameter = DEFAULT_MIN_SCALING_PARAMETER;
        int maxScalingParameter = DEFAULT_MAX_SCALING_PARAMETER;

        String s;
        String staticScalingFactors = options.remove(STATIC_SCALING_FACTORS_OPTION);
        String staticScalingParameters = options.remove(SCALING_PARAMETERS_OPTION);
        if (staticScalingFactors != null && staticScalingParameters != null)
            throw new ConfigurationException(String.format("Either '%s' or '%s' should be used, not both", SCALING_PARAMETERS_OPTION, STATIC_SCALING_FACTORS_OPTION));
        else if (staticScalingFactors != null)
            parseScalingParameters(staticScalingFactors);
        else if (staticScalingParameters != null)
            parseScalingParameters(staticScalingParameters);
        s = options.remove(MIN_SCALING_PARAMETER);
        if (s != null)
            minScalingParameter = Integer.parseInt(s);
        s = options.remove(MAX_SCALING_PARAMETER);
        if (s != null)
            maxScalingParameter = Integer.parseInt(s);

        if (minScalingParameter >= maxScalingParameter || scalingParameter < minScalingParameter || scalingParameter > maxScalingParameter)
            throw new ConfigurationException(String.format("Invalid configuration for the scaling parameter: %d, min: %d, max: %d", scalingParameter, minScalingParameter, maxScalingParameter));

        s = options.remove(INTERVAL_SEC);
        if (s != null)
        {
            int intervalSec = Integer.parseInt(s);
            if (intervalSec <= 0)
                throw new ConfigurationException(String.format("Invalid configuration for interval, it should be positive: %d", intervalSec));
        }
        s = options.remove(THRESHOLD);
        if (s != null)
        {
            double threshold = Double.parseDouble(s);
            if (threshold <= 0 || threshold > 1)
            {
                throw new ConfigurationException(String.format("Invalid configuration for threshold, it should be within (0,1]: %f", threshold));
            }
        }
        s = options.remove(MIN_COST);
        if (s != null)
        {
            int minCost = Integer.parseInt(s);
            if (minCost <= 0)
                throw new ConfigurationException(String.format("Invalid configuration for minCost, it should be positive: %d", minCost));
        }
        s = options.remove(MAX_ADAPTIVE_COMPACTIONS);
        if (s != null)
        {
            int maxAdaptiveCompactions = Integer.parseInt(s);
            if (maxAdaptiveCompactions < -1)
                throw new ConfigurationException(String.format("Invalid configuration for maxAdaptiveCompactions, it should be >= -1 (-1 for no limit): %d", maxAdaptiveCompactions));
        }
        return options;
    }

    @Override
    void startup(UnifiedCompactionStrategy strategy, CostsCalculator calculator)
    {
        super.startup(strategy, calculator);
        this.lastChecked = clock.now();
    }

    @Override
    public int getScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < scalingParameters.length ? scalingParameters[index] : scalingParameters[scalingParameters.length - 1];
    }

    @Override
    public int getPreviousScalingParameter(int index)
    {
        if (index < 0)
            throw new IllegalArgumentException("Index should be >= 0: " + index);

        return index < previousScalingParameters.length ? previousScalingParameters[index] : previousScalingParameters[previousScalingParameters.length - 1];
    }

    @Override
    @Nullable
    public CostsCalculator getCalculator()
    {
        return calculator;
    }

    public int getInterval()
    {
        return intervalSec;
    }

    public int getMinScalingParameter()
    {
        return minScalingParameter;
    }

    public int getMaxScalingParameter()
    {
        return maxScalingParameter;
    }

    public double getThreshold()
    {
        return threshold;
    }

    public int getMinCost()
    {
        return minCost;
    }

    /**
     * Checks to see if the chosen compaction is a result of recent adaptive parameter change.
     * An adaptive compaction is a compaction triggered by changing the scaling parameter W
     */
    @Override
    public boolean isRecentAdaptive(CompactionPick pick)
    {
        int numTables = pick.sstables().size();
        int level = (int) pick.parent();
        return (numTables >= getThreshold(level) && numTables < getPreviousThreshold(level));
    }

    @Override
    public int getMaxRecentAdaptiveCompactions()
    {
        return maxAdaptiveCompactions;
    }

    /** Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks */
    @Override
    public void onStrategyBackgroundTaskRequest()
    {
        if (!isRunning())
            return;

        long now = clock.now();
        if (now - lastChecked < TimeUnit.SECONDS.toNanos(intervalSec))
            return;

        try
        {
            maybeUpdate(now);
        }
        finally
        {
            lastChecked = now;
        }
    }

    /**
     * Maybe updates the scaling parameter according to the data size, read, and write costs.
     *
     * The scaling parameter calculation is based on current read and write query costs for the entire data size.
     * We use the entire data size instead of shard size here because query cost calculations do not take
     * sharding into account. Also, the same scaling parameter is going to be used across all shards.
     *
     * Protected by the synchronized block in UnifiedCompactionStrategy#getNextBackgroundTasks
     *
     * @param now current timestamp only used for debug logging
     */
    private void maybeUpdate(long now)
    {
        final long targetSize = Math.max(getDataSetSizeBytes(), (long) Math.ceil(calculator.spaceUsed()));

        final int RA = readAmplification(targetSize, scalingParameters[0]);
        final int WA = writeAmplification(targetSize, scalingParameters[0]);

        final double readCost = calculator.getReadCostForQueries(RA);
        final double writeCost = calculator.getWriteCostForQueries(WA);
        final double cost =  readCost + writeCost;

        if (cost <= minCost)
        {
            logger.debug("Adaptive compaction controller not updated, cost for current scaling parameter {} is below minimum cost {}: read cost: {}, write cost: {}\nAverages: {}", scalingParameters[0], minCost, readCost, writeCost, calculator);
            return;
        }

        final double[] totCosts = new double[maxScalingParameter - minScalingParameter + 1];
        final double[] readCosts = new double[maxScalingParameter - minScalingParameter + 1];
        final double[] writeCosts = new double[maxScalingParameter - minScalingParameter + 1];
        int candScalingParameter = scalingParameters[0];
        double candCost = cost;

        for (int i = minScalingParameter; i <= maxScalingParameter; i++)
        {
            final int idx = i - minScalingParameter;
            if (i == scalingParameters[0])
            {
                readCosts[idx] = readCost;
                writeCosts[idx] = writeCost;
            }
            else
            {
                final int ra = readAmplification(targetSize, i);
                final int wa = writeAmplification(targetSize, i);

                readCosts[idx] = calculator.getReadCostForQueries(ra);
                writeCosts[idx] = calculator.getWriteCostForQueries(wa);
            }
            totCosts[idx] = readCosts[idx] + writeCosts[idx];
            // in case of a tie, for neg.ve scalingParameters we prefer higher scalingParameters (smaller WA), but not for pos.ve scalingParameters we prefer lower scalingParameters (more parallelism)
            if (totCosts[idx] < candCost || (i < 0 && totCosts[idx] == candCost))
            {
                candScalingParameter = i;
                candCost = totCosts[idx];
            }
        }

        logger.debug("Min cost: {}, min scaling parameter: {}, target sstable size: {}\nread costs: {}\nwrite costs: {}\ntot costs: {}\nAverages: {}",
                     candCost,
                     candScalingParameter,
                     FBUtilities.prettyPrintMemory(getTargetSSTableSize()),
                     Arrays.toString(readCosts),
                     Arrays.toString(writeCosts),
                     Arrays.toString(totCosts),
                     calculator);

        StringBuilder str = new StringBuilder(100);
        str.append("Adaptive compaction controller ");

        if (scalingParameters[0] != candScalingParameter && (cost - candCost) >= threshold * cost)
        {
            //scaling parameter is updated
            str.append("updated ").append(scalingParameters[0]).append(" -> ").append(candScalingParameter);
            this.previousScalingParameters[0] = scalingParameters[0]; //need to keep track of the previous scaling parameter for isAdaptive check
            this.scalingParameters[0] = candScalingParameter;

            //store updated scaling parameters in case a node fails and needs to restart
            storeControllerConfig();
        }
        else if (scalingParameters[0] == candScalingParameter)
        {
            // only update the lowest level that is not equal to candScalingParameter
            // example: candScalingParameter = 4, scalingParameters = {4, 4, 12, 16} --> scalingParameters = {4, 4, 4, 16}
            // as a result, higher levels will be less prone to changes
            for (int i = 1; i < scalingParameters.length; i++)
            {
                if (scalingParameters[i] != candScalingParameter)
                {
                    str.append("updated for level ").append(i).append(": ").append(scalingParameters[i]).append(" -> ").append(candScalingParameter);
                    this.previousScalingParameters[i] = scalingParameters[i];
                    this.scalingParameters[i] = candScalingParameter;

                    //store updated scaling parameters in case a node fails and needs to restart
                    storeControllerConfig();
                    break;
                }
                else if (i == scalingParameters.length-1)
                {
                    str.append("unchanged because all levels have the same scaling parameter");
                }
            }
        }
        else
        {
            //scaling parameter is not updated
            str.append("unchanged");
        }

        str.append(", data size: ").append(FBUtilities.prettyPrintMemory(targetSize));
        str.append(", query cost: ").append(cost);
        str.append(", new query cost: ").append(candCost);
        str.append(", took ").append(TimeUnit.NANOSECONDS.toMicros(clock.now() - now)).append(" us");

        logger.debug(str.toString());
    }

    @Override
    public void storeControllerConfig()
    {
        storeOptions(keyspaceName, tableName, scalingParameters, getFlushSizeBytes());
    }

    @Override
    public String toString()
    {
        return String.format("t: %s, o: %s, scalingParameters: %s - %s", FBUtilities.prettyPrintMemory(targetSSTableSize), Arrays.toString(survivalFactors), Arrays.toString(scalingParameters), calculator);
    }
}
