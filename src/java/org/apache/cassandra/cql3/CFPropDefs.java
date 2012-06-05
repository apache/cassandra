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
package org.apache.cassandra.cql3;

import com.google.common.collect.Sets;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CFPropDefs
{
    private static final Logger logger = LoggerFactory.getLogger(CFPropDefs.class);

    public static final String KW_COMMENT = "comment";
    public static final String KW_READREPAIRCHANCE = "read_repair_chance";
    public static final String KW_DCLOCALREADREPAIRCHANCE = "dclocal_read_repair_chance";
    public static final String KW_GCGRACESECONDS = "gc_grace_seconds";
    public static final String KW_MINCOMPACTIONTHRESHOLD = "min_compaction_threshold";
    public static final String KW_MAXCOMPACTIONTHRESHOLD = "max_compaction_threshold";
    public static final String KW_REPLICATEONWRITE = "replicate_on_write";
    public static final String KW_COMPACTION_STRATEGY_CLASS = "compaction_strategy_class";
    public static final String KW_CACHING = "caching";
    public static final String KW_BF_FP_CHANCE = "bloom_filter_fp_chance";

    // Maps CQL short names to the respective Cassandra comparator/validator class names
    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();
    public static final Set<String> allowedKeywords = new HashSet<String>();

    public static final String COMPACTION_OPTIONS_PREFIX = "compaction_strategy_options";
    public static final String COMPRESSION_PARAMETERS_PREFIX = "compression_parameters";

    static
    {
        keywords.add(KW_COMMENT);
        keywords.add(KW_READREPAIRCHANCE);
        keywords.add(KW_DCLOCALREADREPAIRCHANCE);
        keywords.add(KW_GCGRACESECONDS);
        keywords.add(KW_REPLICATEONWRITE);
        keywords.add(KW_COMPACTION_STRATEGY_CLASS);
        keywords.add(KW_CACHING);
        keywords.add(KW_BF_FP_CHANCE);

        allowedKeywords.addAll(keywords);
        allowedKeywords.addAll(obsoleteKeywords);
    }

    public final Map<String, String> properties = new HashMap<String, String>();
    public final Map<String, String> compactionStrategyOptions = new HashMap<String, String>();
    public final Map<String, String> compressionParameters = new HashMap<String, String>()
    {{
        if (CFMetaData.DEFAULT_COMPRESSOR != null)
            put(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR);
    }};

    public void validate() throws ConfigurationException
    {
        // Catch the case where someone passed a kwarg that is not recognized.
        for (String bogus : Sets.difference(properties.keySet(), allowedKeywords))
            throw new ConfigurationException(bogus + " is not a valid keyword argument for CREATE TABLE");
        for (String obsolete : Sets.intersection(properties.keySet(), obsoleteKeywords))
            logger.warn("Ignoring obsolete property {}", obsolete);
    }

    /** Map a keyword to the corresponding value */
    public void addProperty(String name, String value)
    {
        String[] composite = name.split(":");
        if (composite.length > 1)
        {
            if (composite[0].equals(COMPACTION_OPTIONS_PREFIX))
            {
                compactionStrategyOptions.put(composite[1], value);
                return;
            }
            else if (composite[0].equals(COMPRESSION_PARAMETERS_PREFIX))
            {
                compressionParameters.put(composite[1], value);
                return;
            }
        }
        properties.put(name, value);
    }

    public void addAll(Map<String, String> propertyMap)
    {
        for (Map.Entry<String, String> entry : propertyMap.entrySet())
            addProperty(entry.getKey(), entry.getValue());
    }

    public Boolean hasProperty(String name)
    {
        return properties.containsKey(name);
    }

    public void applyToCFMetadata(CFMetaData cfm) throws ConfigurationException
    {
        if (hasProperty(KW_COMMENT))
            cfm.comment(get(KW_COMMENT));

        cfm.readRepairChance(getDouble(KW_READREPAIRCHANCE, cfm.getReadRepairChance()));
        cfm.dcLocalReadRepairChance(getDouble(KW_DCLOCALREADREPAIRCHANCE, cfm.getDcLocalReadRepair()));
        cfm.gcGraceSeconds(getInt(KW_GCGRACESECONDS, cfm.getGcGraceSeconds()));
        cfm.replicateOnWrite(getBoolean(KW_REPLICATEONWRITE, cfm.getReplicateOnWrite()));
        cfm.minCompactionThreshold(toInt(KW_MINCOMPACTIONTHRESHOLD, compactionStrategyOptions.get(KW_MINCOMPACTIONTHRESHOLD), cfm.getMinCompactionThreshold()));
        cfm.maxCompactionThreshold(toInt(KW_MAXCOMPACTIONTHRESHOLD, compactionStrategyOptions.get(KW_MAXCOMPACTIONTHRESHOLD), cfm.getMaxCompactionThreshold()));
        cfm.caching(CFMetaData.Caching.fromString(getString(KW_CACHING, cfm.getCaching().toString())));
        cfm.bloomFilterFpChance(getDouble(KW_BF_FP_CHANCE, cfm.getBloomFilterFpChance()));

        if (!compactionStrategyOptions.isEmpty())
            cfm.compactionStrategyOptions(new HashMap<String, String>(compactionStrategyOptions));

        if (!compressionParameters.isEmpty())
            cfm.compressionParameters(CompressionParameters.create(compressionParameters));
    }

    public String get(String name)
    {
        return properties.get(name);
    }

    public String getString(String key, String defaultValue)
    {
        String value = properties.get(key);
        return value != null ? value : defaultValue;
    }

    // Return a property value, typed as a Boolean
    public Boolean getBoolean(String key, Boolean defaultValue)
    {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value.toLowerCase().matches("(1|true|yes)");
    }

    // Return a property value, typed as a Double
    public Double getDouble(String key, Double defaultValue) throws ConfigurationException
    {
        Double result;
        String value = properties.get(key);

        if (value == null)
            result = defaultValue;
        else
        {
            try
            {
                result = Double.parseDouble(value);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }

    // Return a property value, typed as an Integer
    public Integer getInt(String key, Integer defaultValue) throws ConfigurationException
    {
        String value = properties.get(key);
        return toInt(key, value, defaultValue);
    }

    public static Integer toInt(String key, String value, Integer defaultValue) throws ConfigurationException
    {
        Integer result;

        if (value == null)
            result = defaultValue;
        else
        {
            try
            {
                result = Integer.parseInt(value);
            }
            catch (NumberFormatException e)
            {
                throw new ConfigurationException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return String.format("CFPropDefs(%s, compaction: %s, compression: %s)",
                             properties.toString(),
                             compactionStrategyOptions.toString(),
                             compressionParameters.toString());
    }
}
