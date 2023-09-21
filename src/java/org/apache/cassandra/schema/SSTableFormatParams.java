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

package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat;
import org.apache.cassandra.utils.BloomCalculations;

import static java.lang.String.format;
import static org.apache.cassandra.schema.SSTableFormatParams.Option.BLOOM_FILTER_FP_CHANCE;
import static org.apache.cassandra.schema.SSTableFormatParams.Option.CRC_CHECK_CHANCE;
import static org.apache.cassandra.schema.SSTableFormatParams.Option.MAX_INDEX_INTERVAL;
import static org.apache.cassandra.schema.SSTableFormatParams.Option.MIN_INDEX_INTERVAL;
import static org.apache.cassandra.schema.SSTableFormatParams.Option.TYPE;
import static org.apache.cassandra.schema.TableParams.fail;

/**
 *  This class describes the params that is used to define the sstable's
 *  format params when modifing the table schema.
 */
public final class SSTableFormatParams
{
    private final ImmutableMap<Option, String> options;;

    public static final String DEFAUL_BIG_TYPE = "big";
    public static final double DEFAULT_CRC_CHECK_CHANCE = 1.0;
    public static final double INVALID_BLOOM_FILTER_FP_CHANCE = -1;
    public static final double DEFAULT_BLOOM_FILTER_FP_CHANCE = 0.01;
    public static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;
    public static final int DEFAULT_MIN_INDEX_INTERVAL = 128;

    private static final Map<String, SSTableFormatParams> CONFIGURATIONS = new HashMap<>();
    public static final SSTableFormatParams DEFAULT_BIG_SSTABLE = SSTableFormatParams.create(ImmutableMap.of(TYPE, DEFAUL_BIG_TYPE,
                                                                                                             BLOOM_FILTER_FP_CHANCE, String.valueOf(DEFAULT_BLOOM_FILTER_FP_CHANCE),
                                                                                                             CRC_CHECK_CHANCE, String.valueOf(DEFAULT_CRC_CHECK_CHANCE),
                                                                                                             MIN_INDEX_INTERVAL, String.valueOf(DEFAULT_MIN_INDEX_INTERVAL),
                                                                                                             MAX_INDEX_INTERVAL, String.valueOf(DEFAULT_MAX_INDEX_INTERVAL)));
    private static final Map<String, SSTableFormatParams>
                        CONFIGURATION_DEFINITIONS = expandDefinitions(DatabaseDescriptor.getSSTableFormatOptions());
    public static final ImmutableSet<String> SSTABLE_FORMAT_TYPES = ImmutableSet.of(BigFormat.NAME, BtiFormat.NAME);
    // big/bti sstable format with non-lcs default bf chance
    public static Function<String, Map<String, String>> DEFAULT_FORMAT_MAP = new Function<String, Map<String, String>>() {
        @Override
        public Map<String, String> apply(String type)
        {
            return  ImmutableMap.of(TYPE.getName(), type,
                                    BLOOM_FILTER_FP_CHANCE.getName(), String.valueOf(DEFAULT_BLOOM_FILTER_FP_CHANCE),
                                    CRC_CHECK_CHANCE.getName(), String.valueOf(DEFAULT_CRC_CHECK_CHANCE),
                                    MIN_INDEX_INTERVAL.getName(), String.valueOf(DEFAULT_MIN_INDEX_INTERVAL),
                                    MAX_INDEX_INTERVAL.getName(), String.valueOf(DEFAULT_MAX_INDEX_INTERVAL));
        }
    };

    // wether these deprecated properties are defined by ddl or not
    private Map<Option, Object> schemaProperties = new HashMap<>();

    public enum Option
    {
        TYPE("type"),
        BLOOM_FILTER_FP_CHANCE("bloom_filter_fp_chance"),
        CRC_CHECK_CHANCE("crc_check_chance"),
        MIN_INDEX_INTERVAL("min_index_interval"),
        MAX_INDEX_INTERVAL("max_index_interval");

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }

        private String name;
        Option(String name)
        {
            this.name = name;
        }
        public String getName()
        {
            return name;
        }
    }

    public SSTableFormatParams(ImmutableMap<Option, String> options)
    {
        this.options = ImmutableMap.copyOf(options);
        validate();
    }

    public void validate()
    {
        if (options.isEmpty())
            throw new ConfigurationException("SSTable format options should not be empty.");

        validateAndParseType(getType());

        double minBloomFilterFpChanceValue = BloomCalculations.minSupportedBloomFilterFpChance();
        double bloomFilterFpChance = getBloomFilterFpChance();
        if (bloomFilterFpChance <=  minBloomFilterFpChanceValue || bloomFilterFpChance > 1)
        {
            fail("%s must be larger than %s and less than or equal to 1.0 (got %s)",
                 BLOOM_FILTER_FP_CHANCE,
                 minBloomFilterFpChanceValue,
                 bloomFilterFpChance);
        }

        double crcCheckChance = getCrcChance();
        if (crcCheckChance < 0 || crcCheckChance > 1.0)
        {
            fail("%s must be larger than or equal to 0 and smaller than or equal to 1.0 (got %s)",
                 CRC_CHECK_CHANCE,
                 crcCheckChance);
        }

        int minIndexInterval = getMinInterval();
        int maxIndexInterval = getMaxInterval();
        if (maxIndexInterval < minIndexInterval)
        {
            fail("%s must be greater than or equal to %s (%s) (got %s)",
                 MAX_INDEX_INTERVAL,
                 MIN_INDEX_INTERVAL,
                 minIndexInterval,
                 maxIndexInterval);
        }
    }

    public String getType()
    {
        return options.get(TYPE);
    }

    public double getBloomFilterFpChance(CompactionParams params)
    {
        return options.containsKey(BLOOM_FILTER_FP_CHANCE) ? Double.valueOf(options.get(BLOOM_FILTER_FP_CHANCE)) : params.defaultBloomFilterFbChance();
    }

    public double getBloomFilterFpChance()
    {
        return Double.valueOf(options.get(BLOOM_FILTER_FP_CHANCE));
    }

    public double getCrcChance()
    {
        return Double.valueOf(options.get(CRC_CHECK_CHANCE));
    }

    public int getMaxInterval()
    {
        return Integer.valueOf(options.get(MAX_INDEX_INTERVAL));
    }

    public int getMinInterval()
    {
        return Integer.valueOf(options.get(MIN_INDEX_INTERVAL));
    }

    public Map<String, String> asMap()
    {
        Map<String, String> map = new TreeMap<>();
        options.forEach((key, value) -> {
            map.put(key.getName(), value);
        });
        return map;
    }

    public static SSTableFormatParams create(ImmutableMap<Option, String> map)
    {
        return new SSTableFormatParams(map);
    }

    public static String validateAndParseType(String name)
    {
        if (name == null)
            throw new ConfigurationException("SSTable format name is null");
        String[] array = name.split("-");
        if (array.length != 2 && array.length != 1)
            throw new ConfigurationException("SSTable format name should be with format like <short-name>[-<variation>] but acctually is " + name );
        if (!array[0].matches("^[a-z]+$"))
            throw new ConfigurationException(String.format("SSTable format name for %s must be non-empty, lower-case letters only string", array[0]));
        return array[0];
    }

    public static SSTableFormatParams get(String key)
    {
        if (key == null)
            key = DEFAUL_BIG_TYPE;

        synchronized (CONFIGURATIONS)
        {
            return CONFIGURATIONS.computeIfAbsent(key, SSTableFormatParams::parseConfiguration);
        }
    }

    public static SSTableFormatParams get(Map<String, String> map)
    {
        String type = map.get(TYPE.getName());
        if (type == null)
            throw new ConfigurationException("sstable_format with map format need type keyword.");
        SSTableFormatParams config = parseConfiguration(type);
        SSTableFormatParams merged = config.merge(map);
        // We do not set the merge value into CONFIGURATIONS as : if user-a set type-a with configuration-a
        // (not same with the configuration-yaml in yaml) at time-1 and next time user-b set type-a will use
        // configuration-a not the default configuration-yaml, but if the node restart and users set type-a agagin
        // then configuration-yaml will be used.
        return merged;
    }

    private static SSTableFormatParams parseConfiguration(String configurationKey)
    {
        SSTableFormatParams definition = CONFIGURATION_DEFINITIONS.get(configurationKey);
        if (definition == null)
            throw new ConfigurationException("SSTableFormat configuration \"" + configurationKey + "\" not found.");
        return definition;
    }

    @VisibleForTesting
    static Map<String, SSTableFormatParams> expandDefinitions(Map<String, Map<String, String>>  ssTableFormatOptions)
    {
        if (ssTableFormatOptions == null)
            return ImmutableMap.of(DEFAUL_BIG_TYPE, DEFAULT_BIG_SSTABLE);

        LinkedHashMap<String, SSTableFormatParams> configs = new LinkedHashMap<>(ssTableFormatOptions.size() + 1);

        if (!ssTableFormatOptions.containsKey(DEFAUL_BIG_TYPE))
            configs.put(DEFAUL_BIG_TYPE, DEFAULT_BIG_SSTABLE);

        for (Map.Entry<String, Map<String, String>> en : ssTableFormatOptions.entrySet())
        {
            Map<String, String> inputMap = new HashMap<>(en.getValue());
            inputMap.put(TYPE.getName(), en.getKey());
            configs.put(en.getKey(), fromMap(inputMap));
        }
        return ImmutableMap.copyOf(configs);
    }

    /**
     * This will return SSTableFormatParams if the properties are all defined
     * in DDL CQL, the these properties will be used, if only some of the properties
     * is defined, then the un-defined properties wil use the value in yaml.
     */
    public static SSTableFormatParams fromMap(Map<String, String> map)
    {
        ImmutableMap.Builder<Option, String> resultBuilder = new ImmutableMap.Builder();
        Option[] options = Option.values();
        for(Option option : options)
        {
            if (option == TYPE && !map.containsKey(option.getName()))
            {
                continue;
            }
            String resultValue = String.valueOf(map.get(option.getName()));
            if (resultValue == null)
                throw new ConfigurationException("Invalid SSTableParmas configuration key" + option.getName());
            resultBuilder.put(option, resultValue);
        }
        return SSTableFormatParams.create(resultBuilder.build());
    }

    private SSTableFormatParams merge(Map<String, String> newInput)
    {
        ImmutableMap.Builder<Option, String> resultBuilder = new ImmutableMap.Builder();
        Option[] options = Option.values();
        for(Option option : options)
        {
            String resultValue = newInput.get(option.getName());
            if (!option.getName().equals(TYPE.getName()))
            {
                resultValue = newInput.containsKey(option.getName()) ? newInput.get(option.getName()) : this.options.get(option);
            }
            resultBuilder.put(option, resultValue);
        }
        return SSTableFormatParams.create(resultBuilder.build());
    }

    /**
     * Set these deprecated properties if they have already been defined in
     * DDL CQL.
     */
    public void setDeprecatedProperties(Map<String, Object> properties)
    {
        for (Option option : Option.values())
        {
            if (option == TYPE)
                continue;
            if (properties.containsKey(option.getName()))
            {
                Object val = properties.get(option.getName());
                if (!(val instanceof String))
                    throw new SyntaxException(format("Invalid value for property '%s'. It should be a string", option.getName()));
                this.schemaProperties.put(option, val);
            }
        }
    }

    /**
     * If deprecated property is defined through cql , and it's value is
     * not the same with the value defined through sstable_format or yaml.
     * Then it is illegal
     */
    public boolean isIllegalDeprecatedProperty(Option option)
    {
        return schemaProperties.containsKey(option) && !(schemaProperties.get(option).equals(options.get(option)));
    }

    public static SSTableFormatParams getWithFallback(Map<String, String> map)
    {
        try
        {
            if (map == null)
            {
                return DEFAULT_BIG_SSTABLE;
            }
            return get(map);
        }
        catch (ConfigurationException e)
        {
            LoggerFactory.getLogger(SSTableFormatParams.class).error("Invalid sstable format configuration \"" + map + "\" in schema." +
                                                                     "Falling back to default to avoid schema mismatch.\n" +
                                                                     "Please ensure the correct definition is given in cassandra.yaml.", e);
            // default value with  setted type name in order to avoid schema missmatch
            return  fromMap(DEFAULT_FORMAT_MAP.apply(map.get(TYPE.getName())));
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
            return true;

        if (!(obj instanceof SSTableFormatParams))
            return false;

        SSTableFormatParams params = (SSTableFormatParams) obj;

        return options.equals(params.options);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(options);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("options", options)
                          .toString();
    }
}