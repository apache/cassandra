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
package org.apache.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.io.compress.CompressionParameters;

/** A <code>CREATE COLUMNFAMILY</code> parsed from a CQL query statement. */
public class CreateColumnFamilyStatement
{
    private final String name;
    private final Map<Term, String> columns = new HashMap<Term, String>();
    private final List<String> keyValidator = new ArrayList<String>();
    private ByteBuffer keyAlias = null;
    private final CFPropDefs cfProps = new CFPropDefs();

    public CreateColumnFamilyStatement(String name)
    {
        this.name = name;
    }

    /** Perform validation of parsed params */
    private void validate(List<ByteBuffer> variables) throws InvalidRequestException
    {
        // Ensure that exactly one key has been specified.
        if (keyValidator.size() < 1)
            throw new InvalidRequestException("You must specify a PRIMARY KEY");
        else if (keyValidator.size() > 1)
            throw new InvalidRequestException("You may only specify one PRIMARY KEY");

        AbstractType<?> comparator;

        try
        {
            cfProps.validate();
            comparator = cfProps.getComparator();
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.toString());
        }
        catch (SyntaxException e)
        {
            throw new InvalidRequestException(e.toString());
        }

        for (Map.Entry<Term, String> column : columns.entrySet())
        {
            ByteBuffer name = column.getKey().getByteBuffer(comparator, variables);

            if (keyAlias != null && keyAlias.equals(name))
                throw new InvalidRequestException("Invalid column name: "
                                                  + column.getKey().getText()
                                                  + ", because it equals to the key_alias.");

        }
    }

    /** Map a column name to a validator for its value */
    public void addColumn(Term term, String comparator)
    {
        columns.put(term, comparator);
    }

    public void setKeyType(String validator)
    {
        keyValidator.add(validator);
    }

    public String getKeyType()
    {
        return keyValidator.get(0);
    }

    public void setKeyAlias(String alias)
    {
        // if we got KEY in input we don't need to set an alias
        if (!alias.toUpperCase().equals("KEY"))
            keyAlias = ByteBufferUtil.bytes(alias);
    }

    /** Map a keyword to the corresponding value */
    public void addProperty(String name, String value)
    {
        cfProps.addProperty(name, value);
    }

    /** Name of the column family to create */
    public String getName()
    {
        return name;
    }

    // Column definitions
    private List<ColumnDefinition> getColumns(CFMetaData cfm) throws InvalidRequestException
    {
        List<ColumnDefinition> columnDefs = new ArrayList<>(columns.size());

        for (Map.Entry<Term, String> col : columns.entrySet())
        {
            try
            {
                ByteBuffer columnName = cfm.comparator.asAbstractType().fromStringCQL2(col.getKey().getText());
                String validatorClassName = CFPropDefs.comparators.containsKey(col.getValue())
                                          ? CFPropDefs.comparators.get(col.getValue())
                                          : col.getValue();
                AbstractType<?> validator = TypeParser.parse(validatorClassName);
                columnDefs.add(ColumnDefinition.regularDef(cfm, columnName, validator, null));
            }
            catch (ConfigurationException e)
            {
                InvalidRequestException ex = new InvalidRequestException(e.toString());
                ex.initCause(e);
                throw ex;
            }
            catch (SyntaxException e)
            {
                InvalidRequestException ex = new InvalidRequestException(e.toString());
                ex.initCause(e);
                throw ex;
            }
        }

        return columnDefs;
    }

    /**
     * Returns a CFMetaData instance based on the parameters parsed from this
     * <code>CREATE</code> statement, or defaults where applicable.
     *
     * @param keyspace keyspace to apply this column family to
     * @return a CFMetaData instance corresponding to the values parsed from this statement
     * @throws InvalidRequestException on failure to validate parsed parameters
     */
    public CFMetaData getCFMetaData(String keyspace, List<ByteBuffer> variables) throws InvalidRequestException
    {
        validate(variables);

        try
        {
            boolean isDense = columns.isEmpty();
            CFMetaData newCFMD = new CFMetaData(keyspace,
                                                name,
                                                ColumnFamilyType.Standard,
                                                CellNames.fromAbstractType(cfProps.getComparator(), isDense));

            if (CFMetaData.DEFAULT_COMPRESSOR != null && cfProps.compressionParameters.isEmpty())
                cfProps.compressionParameters.put(CompressionParameters.SSTABLE_COMPRESSION, CFMetaData.DEFAULT_COMPRESSOR);
            int maxCompactionThreshold = getPropertyInt(CFPropDefs.KW_MAXCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD);
            int minCompactionThreshold = getPropertyInt(CFPropDefs.KW_MINCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD);
            if (minCompactionThreshold <= 0 || maxCompactionThreshold <= 0)
                throw new ConfigurationException("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");

            newCFMD.isDense(isDense)
                   .addAllColumnDefinitions(getColumns(newCFMD))
                   .comment(cfProps.getProperty(CFPropDefs.KW_COMMENT))
                   .readRepairChance(getPropertyDouble(CFPropDefs.KW_READREPAIRCHANCE, CFMetaData.DEFAULT_READ_REPAIR_CHANCE))
                   .dcLocalReadRepairChance(getPropertyDouble(CFPropDefs.KW_DCLOCALREADREPAIRCHANCE, CFMetaData.DEFAULT_DCLOCAL_READ_REPAIR_CHANCE))
                   .gcGraceSeconds(getPropertyInt(CFPropDefs.KW_GCGRACESECONDS, CFMetaData.DEFAULT_GC_GRACE_SECONDS))
                   .defaultValidator(cfProps.getValidator())
                   .minCompactionThreshold(minCompactionThreshold)
                   .maxCompactionThreshold(maxCompactionThreshold)
                   .keyValidator(TypeParser.parse(CFPropDefs.comparators.get(getKeyType())))
                   .compactionStrategyClass(cfProps.compactionStrategyClass)
                   .compactionStrategyOptions(cfProps.compactionStrategyOptions)
                   .compressionParameters(CompressionParameters.create(cfProps.compressionParameters))
                   .caching(CachingOptions.fromString(getPropertyString(CFPropDefs.KW_CACHING, CFMetaData.DEFAULT_CACHING_STRATEGY.toString())))
                   .speculativeRetry(CFMetaData.SpeculativeRetry.fromString(getPropertyString(CFPropDefs.KW_SPECULATIVE_RETRY, CFMetaData.DEFAULT_SPECULATIVE_RETRY.toString())))
                   .bloomFilterFpChance(getPropertyDouble(CFPropDefs.KW_BF_FP_CHANCE, null))
                   .memtableFlushPeriod(getPropertyInt(CFPropDefs.KW_MEMTABLE_FLUSH_PERIOD, 0))
                   .defaultTimeToLive(getPropertyInt(CFPropDefs.KW_DEFAULT_TIME_TO_LIVE, CFMetaData.DEFAULT_DEFAULT_TIME_TO_LIVE));

            // CQL2 can have null keyAliases
            if (keyAlias != null)
                newCFMD.addColumnDefinition(ColumnDefinition.partitionKeyDef(newCFMD, keyAlias, newCFMD.getKeyValidator(), null));

            return newCFMD.rebuild();
        }
        catch (ConfigurationException | SyntaxException e)
        {
            throw new InvalidRequestException(e.toString());
        }
    }

    private String getPropertyString(String key, String defaultValue)
    {
        return cfProps.getPropertyString(key, defaultValue);
    }

    private Boolean getPropertyBoolean(String key, Boolean defaultValue)
    {
        return cfProps.getPropertyBoolean(key, defaultValue);
    }

    private Double getPropertyDouble(String key, Double defaultValue) throws InvalidRequestException
    {
        return cfProps.getPropertyDouble(key, defaultValue);
    }

    private Integer getPropertyInt(String key, Integer defaultValue) throws InvalidRequestException
    {
        return cfProps.getPropertyInt(key, defaultValue);
    }

    private Set<String> getPropertySet(String key, Set<String> defaultValue)
    {
        return cfProps.getPropertySet(key, defaultValue);
    }

    public Map<Term, String> getColumns()
    {
        return columns;
    }

}

