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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.cql3.statements.PropertyDefinitions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.TableParams.Option;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static java.lang.String.format;

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
}
