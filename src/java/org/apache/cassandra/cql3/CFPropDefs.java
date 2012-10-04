/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CFPropDefs extends PropertyDefinitions
{
    private static Logger logger = LoggerFactory.getLogger(CFPropDefs.class);

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
    public static final Map<String, String> comparators = new HashMap<String, String>();
    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();
    public static final Set<String> allowedKeywords = new HashSet<String>();

    public static final String COMPACTION_OPTIONS_PREFIX = "compaction_strategy_options";
    public static final String COMPRESSION_PARAMETERS_PREFIX = "compression_parameters";

    static
    {
        comparators.put("ascii", "AsciiType");
        comparators.put("bigint", "LongType");
        comparators.put("blob", "BytesType");
        comparators.put("boolean", "BooleanType");
        comparators.put("counter", "CounterColumnType");
        comparators.put("decimal", "DecimalType");
        comparators.put("double", "DoubleType");
        comparators.put("float", "FloatType");
        comparators.put("int", "Int32Type");
        comparators.put("text", "UTF8Type");
        comparators.put("timestamp", "DateType");
        comparators.put("uuid", "UUIDType");
        comparators.put("varchar", "UTF8Type");
        comparators.put("varint", "IntegerType");
        comparators.put("timeuuid", "TimeUUIDType");

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

    private Class<? extends AbstractCompactionStrategy> compactionStrategyClass = null;
    public final Map<String, String> compactionStrategyOptions = new HashMap<String, String>();
    public final Map<String, String> compressionParameters = new HashMap<String, String>()
    {{
        if (CFMetaData.DEFAULT_COMPRESSOR != null)
            put(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR);
    }};

    public static AbstractType<?> parseType(String type) throws InvalidRequestException
    {
        try
        {
            String className = comparators.get(type);
            if (className == null)
                className = type;
            return TypeParser.parse(className);
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.toString());
            ex.initCause(e);
            throw ex;
        }
    }

    public void validate() throws ConfigurationException, InvalidRequestException
    {
        validate(keywords, obsoleteKeywords);

        if (properties.containsKey(KW_COMPACTION_STRATEGY_CLASS))
        {
            compactionStrategyClass = CFMetaData.createCompactionStrategy(properties.get(KW_COMPACTION_STRATEGY_CLASS));
            compactionStrategyOptions.remove(KW_COMPACTION_STRATEGY_CLASS);
        }
    }

    @Override
    public void addProperty(String name, String value) throws InvalidRequestException
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
        super.addProperty(name, value);
    }

    public void applyToCFMetadata(CFMetaData cfm) throws ConfigurationException, InvalidRequestException
    {
        if (hasProperty(KW_COMMENT))
            cfm.comment(getString(KW_COMMENT, ""));

        cfm.readRepairChance(getDouble(KW_READREPAIRCHANCE, cfm.getReadRepairChance()));
        cfm.dcLocalReadRepairChance(getDouble(KW_DCLOCALREADREPAIRCHANCE, cfm.getDcLocalReadRepair()));
        cfm.gcGraceSeconds(getInt(KW_GCGRACESECONDS, cfm.getGcGraceSeconds()));
        cfm.replicateOnWrite(getBoolean(KW_REPLICATEONWRITE, cfm.getReplicateOnWrite()));
        cfm.minCompactionThreshold(toInt(KW_MINCOMPACTIONTHRESHOLD, compactionStrategyOptions.get(KW_MINCOMPACTIONTHRESHOLD), cfm.getMinCompactionThreshold()));
        cfm.maxCompactionThreshold(toInt(KW_MAXCOMPACTIONTHRESHOLD, compactionStrategyOptions.get(KW_MAXCOMPACTIONTHRESHOLD), cfm.getMaxCompactionThreshold()));
        cfm.caching(CFMetaData.Caching.fromString(getString(KW_CACHING, cfm.getCaching().toString())));
        cfm.bloomFilterFpChance(getDouble(KW_BF_FP_CHANCE, cfm.getBloomFilterFpChance()));

        if (compactionStrategyClass != null)
        {
            cfm.compactionStrategyClass(compactionStrategyClass);
            cfm.compactionStrategyOptions(new HashMap<String, String>(compactionStrategyOptions));
        }

        cfm.compressionParameters(CompressionParameters.create(compressionParameters));
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
