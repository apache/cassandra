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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.InvalidRequestException;

/** A <code>CREATE COLUMNFAMILY</code> parsed from a CQL query statement. */
public class CreateColumnFamilyStatement
{
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
    private static final String KW_MEMTABLEFLUSHINMINS = "memtable_flush_after_mins";
    private static final String KW_MEMTABLESIZEINMB = "memtable_throughput_in_mb";
    private static final String KW_MEMTABLEOPSINMILLIONS = "memtable_operations_in_millions";
    private static final String KW_REPLICATEONWRITE = "replicate_on_write";
    
    // Maps CQL short names to the respective Cassandra comparator/validator class names
    private static final Map<String, String> comparators = new HashMap<String, String>();
    private static final Set<String> keywords = new HashSet<String>();
    
    static
    {
        comparators.put("bytes", "BytesType");
        comparators.put("ascii", "AsciiType");
        comparators.put("utf8", "UTF8Type");
        comparators.put("int", "IntegerType");
        comparators.put("long", "LongType");
        comparators.put("uuid", "LexicalUUIDType");
        comparators.put("timeuuid", "TimeUUIDType");
        
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
        keywords.add(KW_MEMTABLEFLUSHINMINS);
        keywords.add(KW_MEMTABLESIZEINMB);
        keywords.add(KW_MEMTABLEOPSINMILLIONS);
        keywords.add(KW_REPLICATEONWRITE);
    }
 
    private final String name;
    private final Map<Term, String> columns = new HashMap<Term, String>();
    private final Map<String, String> properties = new HashMap<String, String>();
    
    public CreateColumnFamilyStatement(String name)
    {
        this.name = name;
    }
    
    /** Perform validation of parsed params */
    private void validate() throws InvalidRequestException
    {
        // Catch the case where someone passed a kwarg that is not recognized.
        Set<String> keywordsFound = new HashSet<String>(properties.keySet());
        keywordsFound.removeAll(keywords);
        
        for (String bogus : keywordsFound)
            throw new InvalidRequestException(bogus + " is not a valid keyword argument for CREATE COLUMNFAMILY");
        
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
        
        // Validate memtable settings
        Integer memMins = getPropertyInt(KW_MEMTABLEFLUSHINMINS, null);
        Integer memMb = getPropertyInt(KW_MEMTABLESIZEINMB, null);
        Double memOps = getPropertyDouble(KW_MEMTABLEOPSINMILLIONS, null);
        
        if ((memMins != null) && (memMins <= 0))
            throw new InvalidRequestException(String.format("%s must be non-negative and greater than zero",
                                                            KW_MEMTABLEFLUSHINMINS));
        if ((memMb != null) && (memMb <= 0))
            throw new InvalidRequestException(String.format("%s must be non-negative and greater than zero",
                                                            KW_MEMTABLESIZEINMB));
        if ((memOps != null) && (memOps <=0))
            throw new InvalidRequestException(String.format("%s must be non-negative and greater than zero",
                                                            KW_MEMTABLEOPSINMILLIONS));
    }
    
    /** Map a column name to a validator for its value */
    public void addColumn(Term term, String comparator)
    {
        columns.put(term, comparator);
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
                ByteBuffer columnName = col.getKey().getByteBuffer(comparator);
                String validator = comparators.containsKey(col.getValue()) ? comparators.get(col.getValue()) : col.getValue();
                columnDefs.put(columnName, new ColumnDefinition(columnName, validator, null, null));
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
        
        try
        {
            // RPC uses BytesType as the default validator/comparator but BytesType expects hex for string terms, (not convenient).
            AbstractType<?> comparator = DatabaseDescriptor.getComparator(comparators.get(getPropertyString(KW_COMPARATOR, "ascii")));
            String validator = getPropertyString(KW_DEFAULTVALIDATION, "ascii");
            
            return new CFMetaData(keyspace,
                                  name,
                                  ColumnFamilyType.create("Standard"),
                                  comparator,
                                  null,
                                  properties.get(KW_COMMENT),
                                  getPropertyDouble(KW_ROWCACHESIZE, CFMetaData.DEFAULT_ROW_CACHE_SIZE),
                                  getPropertyDouble(KW_KEYCACHESIZE, CFMetaData.DEFAULT_KEY_CACHE_SIZE),
                                  getPropertyDouble(KW_READREPAIRCHANCE, CFMetaData.DEFAULT_READ_REPAIR_CHANCE),
                                  getPropertyBoolean(KW_REPLICATEONWRITE, false),
                                  getPropertyInt(KW_GCGRACESECONDS, CFMetaData.DEFAULT_GC_GRACE_SECONDS),
                                  DatabaseDescriptor.getComparator(comparators.get(validator)),
                                  getPropertyInt(KW_MINCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MIN_COMPACTION_THRESHOLD),
                                  getPropertyInt(KW_MAXCOMPACTIONTHRESHOLD, CFMetaData.DEFAULT_MAX_COMPACTION_THRESHOLD),
                                  getPropertyInt(KW_ROWCACHESAVEPERIODSECS, CFMetaData.DEFAULT_ROW_CACHE_SAVE_PERIOD_IN_SECONDS),
                                  getPropertyInt(KW_KEYCACHESAVEPERIODSECS, CFMetaData.DEFAULT_KEY_CACHE_SAVE_PERIOD_IN_SECONDS),
                                  getPropertyInt(KW_MEMTABLEFLUSHINMINS, CFMetaData.DEFAULT_MEMTABLE_LIFETIME_IN_MINS),
                                  getPropertyInt(KW_MEMTABLESIZEINMB, CFMetaData.DEFAULT_MEMTABLE_THROUGHPUT_IN_MB),
                                  getPropertyDouble(KW_MEMTABLEOPSINMILLIONS, CFMetaData.DEFAULT_MEMTABLE_OPERATIONS_IN_MILLIONS),
                                  0,
                                  getColumns(comparator));
        }
        catch (ConfigurationException e)
        {
            throw new InvalidRequestException(e.toString());
        }
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
