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
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.MonotonicClock;

/**
 * The static compaction controller periodically checks the IO costs
 * that result from the current configuration of the {@link UnifiedCompactionStrategy}.
 */
public class StaticController extends Controller
{
    /**
     * The scaling parameters W, one per bucket index and separated by a comma.
     * Higher indexes will use the value of the last index with a W specified.
     */
    final static String STATIC_SCALING_PARAMETERS_OPTION = "static_scaling_parameters";
    private final static String DEFAULT_STATIC_SCALING_PARAMETERS = System.getProperty(PREFIX + STATIC_SCALING_PARAMETERS_OPTION, "2");

    private final int[] scalingParameters;

    @VisibleForTesting // comp. simulation
    public StaticController(Environment env,
                            int[] scalingParameters,
                            double[] survivalFactors,
                            long dataSetSizeMB,
                            int numShards,
                            long minSSTableSizeMB,
                            long flushSizeOverrideMB,
                            double maxSpaceOverhead,
                            int maxSSTablesToCompact,
                            long expiredSSTableCheckFrequency,
                            boolean ignoreOverlapsInExpirationCheck,
                            boolean l0ShardsEnabled)
    {
        super(MonotonicClock.preciseTime,
              env,
              survivalFactors,
              dataSetSizeMB,
              numShards,
              minSSTableSizeMB,
              flushSizeOverrideMB,
              maxSpaceOverhead,
              maxSSTablesToCompact,
              expiredSSTableCheckFrequency,
              ignoreOverlapsInExpirationCheck,
              l0ShardsEnabled);
        this.scalingParameters = scalingParameters;
    }

    static Controller fromOptions(Environment env,
                                  double[] survivalFactors,
                                  long dataSetSizeMB,
                                  int numShards,
                                  long minSSTableSizeMB,
                                  long flushSizeOverrideMB,
                                  double maxSpaceOverhead,
                                  int maxSSTablesToCompact,
                                  long expiredSSTableCheckFrequency,
                                  boolean ignoreOverlapsInExpirationCheck,
                                  boolean l0ShardsEnabled,
                                  Map<String, String> options)
    {
        int[] Ws = parseScalingParameters(options.getOrDefault(STATIC_SCALING_PARAMETERS_OPTION, DEFAULT_STATIC_SCALING_PARAMETERS));
        return new StaticController(env,
                                    Ws,
                                    survivalFactors,
                                    dataSetSizeMB,
                                    numShards,
                                    minSSTableSizeMB,
                                    flushSizeOverrideMB,
                                    maxSpaceOverhead,
                                    maxSSTablesToCompact,
                                    expiredSSTableCheckFrequency,
                                    ignoreOverlapsInExpirationCheck,
                                    l0ShardsEnabled);
    }

    @VisibleForTesting
    static int[] parseScalingParameters(String str)
    {
        String[] vals = str.split(",");
        int[] ret = new int[vals.length];
        for (int i = 0; i < vals.length; i++)
            ret[i] = Integer.parseInt(vals[i].trim());

        return ret;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        String s = options.remove(STATIC_SCALING_PARAMETERS_OPTION);
        if (s != null)
            parseScalingParameters(s);
        return options;
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
        //scalingParameters is not updated in StaticController so previous scalingParameters = scalingParameters
        return getScalingParameter(index);
    }

    @Override
    public int getMaxAdaptiveCompactions()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public String toString()
    {
        return String.format("Static controller, m: %d, o: %s, Ws: %s, cost: %s", minSstableSizeMB, Arrays.toString(survivalFactors), Arrays.toString(scalingParameters), calculator);
    }
}