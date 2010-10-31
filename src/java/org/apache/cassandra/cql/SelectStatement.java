package org.apache.cassandra.cql;
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


import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 * 
 * @author eevans
 *
 */
public class SelectStatement
{
    private final String columnFamily;
    private final ConsistencyLevel cLevel;
    private final SelectExpression expression;
    private final int numRecords;
    private final int numColumns;
    private final boolean reverse;
    
    public SelectStatement(String columnFamily, ConsistencyLevel cLevel, SelectExpression expression,
            int numRecords, int numColumns, boolean reverse)
    {
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.expression = expression;
        this.numRecords = numRecords;
        this.numColumns = numColumns;
        this.reverse = reverse;
    }
    
    public Predicates getKeyPredicates()
    {
        return expression.getKeyPredicates();
    }
    
    public Predicates getColumnPredicates()
    {
        return expression.getColumnPredicates();
    }
    
    public String getColumnFamily()
    {
        return columnFamily;
    }
    
    public boolean reversed()
    {
        return reverse;
    }
    
    public ConsistencyLevel getConsistencyLevel()
    {
        return cLevel;
    }

    public int getNumRecords()
    {
        return numRecords;
    }

    public int getNumColumns()
    {
        return numColumns;
    }
}
