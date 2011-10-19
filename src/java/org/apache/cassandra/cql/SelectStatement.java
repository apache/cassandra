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
import java.util.List;
import java.util.Set;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Encapsulates a completely parsed SELECT query, including the target
 * column family, expression, result count, and ordering clause.
 *
 */
public class SelectStatement
{
    private final SelectExpression expression;
    private final boolean isCountOper;
    private final String columnFamily;
    private final String keyspace;
    private final ConsistencyLevel cLevel;
    private final WhereClause clause;
    private final int numRecords;
    
    public SelectStatement(SelectExpression expression, boolean isCountOper, String keyspace, String columnFamily,
            ConsistencyLevel cLevel, WhereClause clause, int numRecords)
    {
        this.expression = expression;
        this.isCountOper = isCountOper;
        this.keyspace = keyspace;
        this.columnFamily = columnFamily;
        this.cLevel = cLevel;
        this.clause = (clause != null) ? clause : new WhereClause();
        this.numRecords = numRecords;
    }
    
    public boolean isKeyRange()
    {
        return clause.isKeyRange();
    }
    
    public Set<Term> getKeys()
    {
        return clause.getKeys();
    }
    
    public Term getKeyStart()
    {
        return clause.getStartKey();
    }
    
    public Term getKeyFinish()
    {
        return clause.getFinishKey();
    }
    
    public List<Relation> getColumnRelations()
    {
        return clause.getColumnRelations();
    }
    
    public boolean isColumnRange()
    {
        return expression.isColumnRange();
    }

    public boolean isWildcard()
    {
        return expression.isWildcard();
    }
    
    public List<Term> getColumnNames()
    {
        return expression.getColumns();
    }
    
    public Term getColumnStart()
    {
        return expression.getStart();
    }
    
    public Term getColumnFinish()
    {
        return expression.getFinish();
    }

    public boolean isSetKeyspace()
    {
        return keyspace != null;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getColumnFamily()
    {
        return columnFamily;
    }
    
    public boolean isColumnsReversed()
    {
        return expression.isColumnsReversed();
    }
    
    public ConsistencyLevel getConsistencyLevel()
    {
        return cLevel;
    }

    public int getNumRecords()
    {
        return numRecords;
    }

    public int getColumnsLimit()
    {
        return expression.getColumnsLimit();
    }
    
    public boolean isCountOperation()
    {
        return isCountOper;
    }

    public boolean includeStartKey()
    {
        return clause.includeStartKey();
    }

    public boolean includeFinishKey()
    {
        return clause.includeFinishKey();
    }

    public String getKeyAlias()
    {
        return clause.getKeyAlias();
    }

    public boolean isMultiKey()
    {
        return clause.isMultiKey();
    }

    public void extractKeyAliasFromColumns(CFMetaData cfm)
    {
        clause.extractKeysFromColumns(cfm);
    }

    public AbstractType getComparator(String keyspace)
    {
        return Schema.instance.getComparator(keyspace, columnFamily);
    }
    
    public AbstractType getValueValidator(String keyspace, ByteBuffer column)
    {
        return Schema.instance.getValueValidator(keyspace, columnFamily, column);
    }

}
