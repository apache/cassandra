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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.db.compaction.LeveledCompactionStrategy;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.AutoRepairParams;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.TableParams.Option;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement.ire;

public final class TableAttributes extends PropertyDefinitions
{
    public static final String ID = "id";
    private static final Set<String> validKeywords;
    private static final Set<String> obsoleteKeywords;

    static
    {
        ImmutableSet.Builder<String> validBuilder = ImmutableSet.builder();
        for (Option option : Option.values())
            validBuilder.add(option.toString());
        validBuilder.add(ID);
        validKeywords = validBuilder.build();
        obsoleteKeywords = ImmutableSet.of();
    }

    public void validate()
    {
        validate(validKeywords, obsoleteKeywords);
        build(TableParams.builder()).validate();
    }

    TableParams asNewTableParams()
    {
        return build(TableParams.builder());
    }

    TableParams asAlteredTableParams(TableParams previous)
    {
        if (getId() != null)
            throw new ConfigurationException("Cannot alter table id.");
        return build(previous.unbuild());
    }

    public TableId getId() throws ConfigurationException
    {
        String id = getSimple(ID);
        try
        {
            return id != null ? TableId.fromString(id) : null;
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Invalid table id", e);
        }
    }

    public static Set<String> validKeywords()
    {
        return ImmutableSet.copyOf(validKeywords);
    }

    public static Set<String> allKeywords()
    {
        return Sets.union(validKeywords, obsoleteKeywords);
    }

    private TableParams build(TableParams.Builder builder)
    {
        if (hasOption(Option.BLOOM_FILTER_FP_CHANCE))
            builder.bloomFilterFpChance(getDouble(Option.BLOOM_FILTER_FP_CHANCE));

        if (hasOption(Option.CACHING))
            builder.caching(CachingParams.fromMap(getMap(Option.CACHING)));

        if (hasOption(Option.COMMENT))
            builder.comment(getString(Option.COMMENT));

        if (hasOption(Option.COMPACTION))
            builder.compaction(CompactionParams.fromMap(getMap(Option.COMPACTION)));

        if (hasOption(Option.COMPRESSION))
        {
            //crc_check_chance was "promoted" from a compression property to a top-level-property after #9839
            //so we temporarily accept it to be defined as a compression option, to maintain backwards compatibility
            Map<String, String> compressionOpts = getMap(Option.COMPRESSION);
            if (compressionOpts.containsKey(Option.CRC_CHECK_CHANCE.toString().toLowerCase()))
            {
                Double crcCheckChance = getDeprecatedCrcCheckChance(compressionOpts);
                builder.crcCheckChance(crcCheckChance);
            }
            builder.compression(CompressionParams.fromMap(getMap(Option.COMPRESSION)));
        }

        if (hasOption(Option.MEMTABLE))
            builder.memtable(MemtableParams.get(getString(Option.MEMTABLE)));

        if (hasOption(Option.DEFAULT_TIME_TO_LIVE))
            builder.defaultTimeToLive(getInt(Option.DEFAULT_TIME_TO_LIVE));

        if (hasOption(Option.GC_GRACE_SECONDS))
            builder.gcGraceSeconds(getInt(Option.GC_GRACE_SECONDS));

        if (hasOption(Option.MAX_INDEX_INTERVAL))
            builder.maxIndexInterval(getInt(Option.MAX_INDEX_INTERVAL));

        if (hasOption(Option.MEMTABLE_FLUSH_PERIOD_IN_MS))
            builder.memtableFlushPeriodInMs(getInt(Option.MEMTABLE_FLUSH_PERIOD_IN_MS));

        if (hasOption(Option.MIN_INDEX_INTERVAL))
            builder.minIndexInterval(getInt(Option.MIN_INDEX_INTERVAL));

        if (hasOption(Option.SPECULATIVE_RETRY))
            builder.speculativeRetry(SpeculativeRetryPolicy.fromString(getString(Option.SPECULATIVE_RETRY)));

        if (hasOption(Option.ADDITIONAL_WRITE_POLICY))
            builder.additionalWritePolicy(SpeculativeRetryPolicy.fromString(getString(Option.ADDITIONAL_WRITE_POLICY)));

        if (hasOption(Option.CRC_CHECK_CHANCE))
            builder.crcCheckChance(getDouble(Option.CRC_CHECK_CHANCE));

        if (hasOption(Option.CDC))
            builder.cdc(getBoolean(Option.CDC.toString(), false));

        if (hasOption(Option.READ_REPAIR))
            builder.readRepair(ReadRepairStrategy.fromString(getString(Option.READ_REPAIR)));

        if (hasOption(Option.STRICT_MV_CONSISTENCY))
            builder.strictMVConsistency(getBoolean(Option.STRICT_MV_CONSISTENCY.toString(), false));

        if (hasOption(Option.AUTO_REPAIR))
            builder.automatedRepair(AutoRepairParams.fromMap(getMap(Option.AUTO_REPAIR)));

        return builder.build();
    }

    private Double getDeprecatedCrcCheckChance(Map<String, String> compressionOpts)
    {
        String value = compressionOpts.get(Option.CRC_CHECK_CHANCE.toString().toLowerCase());
        try
        {
            return Double.valueOf(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(String.format("Invalid double value %s for crc_check_chance.'", value));
        }
    }

    private double getDouble(Option option)
    {
        String value = getString(option);

        try
        {
            return Double.parseDouble(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(format("Invalid double value %s for '%s'", value, option));
        }
    }

    private int getInt(Option option)
    {
        String value = getString(option);

        try
        {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new SyntaxException(String.format("Invalid integer value %s for '%s'", value, option));
        }
    }

    private String getString(Option option)
    {
        String value = getSimple(option.toString());
        if (value == null)
            throw new IllegalStateException(format("Option '%s' is absent", option));
        return value;
    }

    private Map<String, String> getMap(Option option)
    {
        Map<String, String> value = getMap(option.toString());
        if (value == null)
            throw new IllegalStateException(format("Option '%s' is absent", option));
        return value;
    }

    public boolean hasOption(Option option)
    {
        return hasProperty(option.toString());
    }

    /**
     * Overwrite the TableAttribtues properties. Use LCS with default sstable size from {@link org.apache.cassandra.config.Config#lcs_sstable_size_in_mb}
     */
    public void setLCS()
    {
        Map<String, String> defaultLCSOptions = new HashMap<>(Collections.emptyMap());
        defaultLCSOptions.put(CompactionParams.Option.CLASS.toString(), LeveledCompactionStrategy.class.getSimpleName());
        properties.put(Option.COMPACTION.toString(), defaultLCSOptions);
        overrideLCSSSTableSizeInMb(DatabaseDescriptor.getLCSSSTableSizeInMB());
    }

    /**
     * (Uber-specific) Apply Zstd compression enforcement while preserving user-specified chunk_length
     */
    public void applyZstdEnforcement(String keyspaceName, String tableName)
    {
        // should not affect any system schema behavior
        if (SchemaConstants.isSystemKeyspace(keyspaceName) || !DatabaseDescriptor.getEnforceZstdCompression()) {
            return;
        }

        // Create fresh compression options map with Zstd settings
        Map<String, String> compressionOptions = new HashMap<>();
        compressionOptions.put(CompressionParams.CLASS, ZstdCompressor.class.getSimpleName());
        compressionOptions.put(ZstdCompressor.COMPRESSION_LEVEL_OPTION_NAME,
                              String.valueOf(DatabaseDescriptor.getEnforceZstdCompressionLevel()));

        // Preserve only standard compression parameters from user options
        if (hasOption(Option.COMPRESSION))
        {
            Map<String, String> userOptions = getMap(Option.COMPRESSION);

            // Preserve chunk_length_in_kb if specified
            if (userOptions.containsKey(CompressionParams.CHUNK_LENGTH_IN_KB))
            {
                compressionOptions.put(CompressionParams.CHUNK_LENGTH_IN_KB,
                                      userOptions.get(CompressionParams.CHUNK_LENGTH_IN_KB));
            }

            // Preserve min_compress_ratio if specified
            if (userOptions.containsKey(CompressionParams.MIN_COMPRESS_RATIO))
            {
                compressionOptions.put(CompressionParams.MIN_COMPRESS_RATIO,
                                      userOptions.get(CompressionParams.MIN_COMPRESS_RATIO));
            }
        }

        properties.put(Option.COMPRESSION.toString(), compressionOptions);
        logger.info(String.format("Zstd compression enforcement is enabled. Setting ZstdCompressor with compression_level: %d for %s.%s.",
                                  DatabaseDescriptor.getEnforceZstdCompressionLevel(), keyspaceName, tableName));
    }

    public boolean overrideLCSSSTableSizeInMb(int size)
    {
        boolean overriden = false;
        Map<String, String> csOptions = getMap(Option.COMPACTION);
        if (csOptions.containsKey(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION))
            overriden = true;
        csOptions.put(LeveledCompactionStrategy.SSTABLE_SIZE_OPTION, String.valueOf(size));
        properties.put(Option.COMPACTION.toString(), csOptions);
        return overriden;
    }

    public String getCompactionStrategy()
    {
        Map<String, String> csOptions = getMap(Option.COMPACTION);
        return csOptions.get(CompactionParams.Option.CLASS.toString());
    }

    public void applyLCSEnforcement(String keyspaceName, String tableName)
    {
        // should not affect any system schema behavior
        if (SchemaConstants.isSystemKeyspace(keyspaceName) ||
            DatabaseDescriptor.getLCSEnforcementLevel() == Config.LCSEnforcementLevel.none) {
            logger.info(String.format("LCS enforcement level=%s. Creating %s.%s with original setting.",
                                      DatabaseDescriptor.getLCSEnforcementLevel().name(), keyspaceName, tableName));
            return;
        }

        if (!hasOption(TableParams.Option.COMPACTION))
        {
            // set LCS if statement with no comapction strategy declared
            setLCS();
            logger.info(String.format("LCS enforcement is enabled (level=%s). Setting LCS for %s.%s as no compaction strategy is specified.",
                                      DatabaseDescriptor.getLCSEnforcementLevel().toString(), keyspaceName, tableName));
        }
        else if (!Objects.equals(getCompactionStrategy(), LeveledCompactionStrategy.class.getSimpleName()) &&
                 !Objects.equals(getCompactionStrategy(), LeveledCompactionStrategy.class.getName()))
        {
            if (DatabaseDescriptor.getLCSEnforcementLevel() == Config.LCSEnforcementLevel.hard) {
                // throw exception hard flag is used for LCS enforcement
                throw ire("LCS enforcement is enabled. You're trying to create schema with %s for %s.%s. Please use " +
                          "LeveledCompactionStrategy for your schema, or contact Cassandra " +
                          "team for assistance creating non-LCS schema.", getCompactionStrategy(), keyspaceName, tableName);
            } else if (DatabaseDescriptor.getLCSEnforcementLevel() == Config.LCSEnforcementLevel.soft) {
                // silently transform to LCS if soft flag is used for LCS enforcement
                setLCS();
                logger.info(String.format("LCS enforcement is enabled (level=%s). Transforming to LCS for %s.%s.",
                                          DatabaseDescriptor.getLCSEnforcementLevel().name(), keyspaceName, tableName));
            }
        }
        else
        {
            // CREATE with LCS
            if (overrideLCSSSTableSizeInMb(DatabaseDescriptor.getLCSSSTableSizeInMB()))
                logger.warn(String.format("sstable_size_in_mb is overriden with %s for performance concern, details in CASSANDRA-19596",
                                          DatabaseDescriptor.getLCSSSTableSizeInMB()));
        }
    }
}
