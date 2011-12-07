/*
 * 
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
 * 
 */
package org.apache.cassandra.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/** A <code>CREATE COLUMNFAMILY</code> parsed from a CQL query statement. */
public class CreateColumnFamilyStatement
{
    private static Logger logger = LoggerFactory.getLogger(CreateColumnFamilyStatement.class);

    private static final String KW_COMPARATOR = "comparator";
    private static final String KW_COMMENT = "comment";
    private static final String KW_ROWCACHESIZE = "row_cache_size";
    private static final String KW_KEYCACHESIZE = "key_cache_size";
    private static final String KW_READREPAIRCHANCE = "read_repair_chance";
    private static final String KW_GCGRACESECONDS = "gc_grace_seconds";
    private static final String KW_DEFAULTVALIDATION = "default_validation";
    private static final String KW_MINCOMPACTIONTHRESHOLD = "min_compaction_threshold";
    private static final String KW_MAXCOMPACTIONTHRESHOLD = "max_compaction_threshold";
    private static final String KW_ROWCACHESAVEPERIODSECS = "row_cache_save_period_in_seconds";
    private static final String KW_KEYCACHESAVEPERIODSECS = "key_cache_save_period_in_seconds";
    private static final String KW_REPLICATEONWRITE = "replicate_on_write";
    private static final String KW_ROW_CACHE_PROVIDER = "row_cache_provider";
    
    // Maps CQL short names to the respective Cassandra comparator/validator class names
    public  static final Map<String, String> comparators = new HashMap<String, String>();
    private static final Set<String> keywords = new HashSet<String>();
    private static final Set<String> obsoleteKeywords = new HashSet<String>();
    
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

        keywords.add(KW_COMPARATOR);
        keywords.add(KW_COMMENT);
        keywords.add(KW_ROWCACHESIZE);
        keywords.add(KW_KEYCACHESIZE);
        keywords.add(KW_READREPAIRCHANCE);
        keywords.add(KW_GCGRACESECONDS);
        keywords.add(KW_DEFAULTVALIDATION);
        keywords.add(KW_MINCOMPACTIONTHRESHOLD);
        keywords.add(KW_MAXCOMPACTIONTHRESHOLD);
        keywords.add(KW_ROWCACHESAVEPERIODSECS);
        keywords.add(KW_KEYCACHESAVEPERIODSECS);
        keywords.add(KW_REPLICATEONWRITE);
        keywords.add(KW_ROW_CACHE_PROVIDER);

        obsoleteKeywords.add("memtable_throughput_in_mb");
        obsoleteKeywords.add("memtable_operations_in_millions");
        obsoleteKeywords.add("memtable_flush_after_mins");
    }
 
    private final String name;
    private final Map<Term, String> columns = new HashMap<Term, String>();
    private final Map<String, String> properties = new HashMap<String, String>();
    private List<String> keyValidator = new ArrayList<String>();
    private ByteBuffer keyAlias = null;

    public CreateColumnFamilyStatement(String name)
    {
        this.name = name;
    }
    
    /** Perform validation of parsed params */
    private void validate() throws InvalidRequestException
    {
        // Column family name
        if (!name.matches("\\w+"))
            throw new InvalidRequestException(String.format("\"%s\" is not a valid column family name", name));
        
        // Catch the case where someone passed a kwarg that is not recognized.
        for (String bogus : Sets.difference(properties.keySet(), Sets.union(keywords, obsoleteKeywords)))
            throw new InvalidRequestException(bogus + " is not a valid keyword argument for CREATE COLUMNFAMILY");
        for (String obsolete : Sets.intersection(properties.keySet(), obsoleteKeywords))
            logger.warn("Ignoring obsolete property {}", obsolete);
        
        // Validate min/max compaction thresholds
        Integer minCompaction = getPropertyInt(KW_MINCOMPACTIONTHRESHOLD, null);
        Integer maxCompaction = getPropertyInt(KW_MAXCOMPACTIONTHRESHOLD, null);
        
        if ((minCompaction != null) && (maxCompaction != null))     // Both min and max are set
        {
            if ((minCompaction > maxCompaction) && (maxCompaction != 0))
                throw new InvalidRequestException(String.format("%s cannot be larger than %s",
                                                                KW_MINCOMPACTIONTHRESHOLD,
                                                                KW_MAXCOMPACTIONTHRESHOLD));
        }
        else if (minCompaction != null)     // Only the min threshold is set
        {
            if (minCompaction > CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD)
                throw new InvalidRequestException(String.format("%s cannot be larger than %s, (default %s)",
                                                                KW_MINCOMPACTIONTHRESHOLD,
                                                                KW_MAXCOMPACTIONTHRESHOLD,
                                                                CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD));
        }
        else if (maxCompaction != null)     // Only the max threshold is set
        {
            if ((maxCompaction < CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD) && (maxCompaction != 0))
                throw new InvalidRequestException(String.format("%s cannot be smaller than %s, (default %s)",
                                                                KW_MAXCOMPACTIONTHRESHOLD,
                                                                KW_MINCOMPACTIONTHRESHOLD,
                                                                CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD));
        }
        
        // Ensure that exactly one key has been specified.
        if (keyValidator.size() < 1)
            throw new InvalidRequestException("You must specify a PRIMARY KEY");
        else if (keyValidator.size() > 1)
            throw new InvalidRequestException("You may only specify one PRIMARY KEY");

        AbstractType<?> comparator;

        try
        {
            comparator = getComparator();
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.toString());
        }

        for (Map.Entry<Term, String> column : columns.entrySet())
        {
            ByteBuffer name = column.getKey().getByteBuffer(comparator);

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
        properties.put(name, value);
    }
    
    /** Name of the column family to create */
    public String getName()
    {
        return name;
    }
    
    // Column definitions
    private Map<ByteBuffer, ColumnDefinition> getColumns(AbstractType<?> comparator) throws InvalidRequestException
    {
        Map<ByteBuffer, ColumnDefinition> columnDefs = new HashMap<ByteBuffer, ColumnDefinition>();
        
        for (Map.Entry<Term, String> col : columns.entrySet())
        {
            try
            {
                ByteBuffer columnName = comparator.fromString(col.getKey().getText());
                String validatorClassName = comparators.containsKey(col.getValue()) ? comparators.get(col.getValue()) : col.getValue();
                AbstractType<?> validator = TypeParser.parse(validatorClassName);
                columnDefs.put(columnName, new ColumnDefinition(columnName, validator, null, null, null));
            }
            catch (ConfigurationException e)
            {
                InvalidRequestException ex = new InvalidRequestException(e.toString());
                ex.initCause(e);
                throw ex;
            }
        }
        
        return columnDefs;
    }

    /* If not comparator/validator is not specified, default to text (BytesType is the wrong default for CQL
     * since it uses hex terms).  If the value specified is not found in the comparators map, assume the user
     * knows what they are doing (a custom comparator/validator for example), and pass it on as-is.
     */

    private AbstractType<?> getComparator() throws ConfigurationException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_COMPARATOR, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_COMPARATOR, "text"))
                                  : getPropertyString(KW_COMPARATOR, "text"));
    }

    private AbstractType<?> getValidator() throws ConfigurationException
    {
        return TypeParser.parse((comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text")) != null)
                                  ? comparators.get(getPropertyString(KW_DEFAULTVALIDATION, "text"))
                                  : getPropertyString(KW_DEFAULTVALIDATION, "text"));
    }

    /**
     * Returns a CFMetaData instance based on the parameters parsed from this
     * <code>CREATE</code> statement, or defaults where applicable.
     * 
     * @param keyspace keyspace to apply this column family to
     * @return a CFMetaData instance corresponding to the values parsed from this statement
     * @throws InvalidRequestException on failure to validate parsed parameters
     */
    public CFMetaData getCFMetaData(String keyspace) throws InvalidRequestException
    {
        validate();

        CFMetaData newCFMD;
        try
        {
            AbstractType<?> comparator = getComparator();

            newCFMD = new CFMetaData(keyspace,
                                     name,
                                     ColumnFamilyType.Standard,
                                     comparator,
                                     null);

            newCFMD.comment(properties.get(KW_COMMENT))
                   .rowCacheSize(getPropertyDouble(KW_ROWCACHESIZE, CFMetaData.DEFAULT_ROW_CACHE_SIZE))
                   .keyCacheSize(getPropertyDouble(KW_KEYCACHESIZE, CFMetaData.DEFAULT_KEY_CACHE_SIZE))
                   .readRepairChance(getPropertyDouble(KW_READREPAIRCHANCE, CFMetaData.DEFAULT_READ_REPAIR_CHANCE))
                   .replicateOnWrite(getPropertyBoolean(KW_REPLICATEONWRITE, CFMetaData.DEFAULT_REPLICATE_ON_WRITE))
                   .gcGraceSeconds(getPropertyInt(KW_GCGRACESECONDS, CFMetaData.DEFAULT_GC_GRACE_SECONDS))
                   .defaultValidator(getValidator())
                   .minCompactionThreshold(getPropertyInt(KW_MINCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD))
                   .maxCompactionThreshold(getPropertyInt(KW_MAXCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD))
                   .rowCacheSavePeriod(getPropertyInt(KW_ROWCACHESAVEPERIODSECS, CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS))
                   .keyCacheSavePeriod(getPropertyInt(KW_KEYCACHESAVEPERIODSECS, CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS))
                   .mergeShardsChance(0.0)
                   .columnMetadata(getColumns(comparator))
                   .keyValidator(TypeParser.parse(comparators.get(getKeyType())))
                   .rowCacheProvider(FBUtilities.newCacheProvider(getPropertyString(KW_ROW_CACHE_PROVIDER, CFMetaData.DEFAULT_ROW_CACHE_PROVIDER.getClass().getName())))
                   .keyAlias(keyAlias)
                   .validate();
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.toString());
        }
        return newCFMD;
    }
    
    private String getPropertyString(String key, String defaultValue)
    {
        String value = properties.get(key);
        return value != null ? value : defaultValue;
    }
    
    // Return a property value, typed as a Boolean
    private Boolean getPropertyBoolean(String key, Boolean defaultValue) throws InvalidRequestException
    {
        String value = properties.get(key);
        return (value == null) ? defaultValue : value.toLowerCase().matches("(1|true|yes)");
    }
    
    // Return a property value, typed as a Double
    private Double getPropertyDouble(String key, Double defaultValue) throws InvalidRequestException
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
                throw new InvalidRequestException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }
    
    // Return a property value, typed as an Integer
    private Integer getPropertyInt(String key, Integer defaultValue) throws InvalidRequestException
    {
        Integer result;
        String value = properties.get(key);
        
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
                throw new InvalidRequestException(String.format("%s not valid for \"%s\"", value, key));
            }
        }
        return result;
    }
}
