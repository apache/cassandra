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
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement
{
    public static final ConsistencyLevel defaultConsistency = ConsistencyLevel.ONE;
    private String columnFamily;
    private ConsistencyLevel cLevel = null;
    private Map<Term, Term> columns;
    private List<Term> columnNames, columnValues;
    private Term key;
    
    /**
     * Creates a new UpdateStatement from a column family name, columns map, consistency
     * level, and key term.
     * 
     * @param columnFamily column family name
     * @param cLevel the thrift consistency level
     * @param columns a map of column name/values pairs
     * @param key the key name
     */
    public UpdateStatement(String columnFamily, ConsistencyLevel cLevel, Map<Term, Term> columns, Term key)
    {
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.columns = columns;
        this.key = key;
    }

    /**
     * Creates a new UpdateStatement from a column family name, columns map,
     * and key term.
     * 
     * @param columnFamily column family name
     * @param columns a map of column name/values pairs
     * @param key the key name
     */
    public UpdateStatement(String columnFamily, Map<Term, Term> columns, Term key)
    {
        this(columnFamily, null, columns, key);
    }
    
    /**
     * Creates a new UpdateStatement from a column family name, a consistency level,
     * key, and lists of column names and values.  It is intended for use with the
     * alternate update format, <code>INSERT</code>.
     * 
     * @param columnFamily column family name
     * @param cLevel the thrift consistency level
     * @param columnNames list of column names
     * @param columnValues list of column values (corresponds to names)
     * @param key the key name
     */
    public UpdateStatement(String columnFamily, ConsistencyLevel cLevel, List<Term> columnNames, List<Term> columnValues, Term key)
    {
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.columnNames = columnNames;
        this.columnValues = columnValues;
        this.key = key;
    }

    /**
     * Returns the consistency level of this <code>UPDATE</code> statement, either
     * one parsed from the CQL statement, or the default level otherwise.
     * 
     * @return the consistency level as a Thrift enum.
     */
    public ConsistencyLevel getConsistencyLevel()
    {
        return (cLevel != null) ? cLevel : defaultConsistency;
    }
    
    /**
     * True if an explicit consistency level was parsed from the statement.
     * 
     * @return true if a consistency was parsed, false otherwise.
     */
    public boolean isSetConsistencyLevel()
    {
        return (cLevel != null);
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }
    
    public Term getKey()
    {
        return key;
    }
    
    public Map<Term, Term> getColumns() throws InvalidRequestException
    {
        // Created from an UPDATE
        if (columns != null)
            return columns;
        
        // Created from an INSERT
        
        // Don't hate, validate.
        if (columnNames.size() != columnValues.size())
            throw new InvalidRequestException("unmatched column names/values");
        if (columnNames.size() < 1)
            throw new InvalidRequestException("no columns specified for INSERT");
        
        columns = new HashMap<Term, Term>();
        
        for (int i = 0; i < columnNames.size(); i++)
            columns.put(columnNames.get(i), columnValues.get(i));
        
        return columns;
    }
    
    public String toString()
    {
        return String.format("UpdateStatement(columnFamily=%s, key=%s, columns=%s, consistency=%s)",
                             columnFamily,
                             key,
                             columns,
                             cLevel);
    }
    
    public AbstractType<?> getKeyType(String keyspace)
    {
        return DatabaseDescriptor.getCFMetaData(keyspace, columnFamily).getKeyValidator();
    }
    
    public AbstractType<?> getComparator(String keyspace)
    {
        return DatabaseDescriptor.getComparator(keyspace, columnFamily);
    }
    
    public AbstractType<?> getValueValidator(String keyspace, ByteBuffer column)
    {
        return DatabaseDescriptor.getValueValidator(keyspace, columnFamily, column);
    }
}
