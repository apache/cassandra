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


import java.util.ArrayList;
import java.util.List;

/**
 * WhereClauses encapsulate all of the predicates of a SELECT query.
 *
 */
public class WhereClause
{
    private List<Term> keys = new ArrayList<Term>();
    private Term startKey, finishKey;
    private List<Relation> columns = new ArrayList<Relation>();
    
    /**
     * Create a new WhereClause with the first parsed relation.
     * 
     * @param firstRelation key or column relation
     */
    public WhereClause(Relation firstRelation)
    {
        and(firstRelation);
    }
    
    public WhereClause()
    {
        
    }
    
    /**
     * Add an additional relation to this WHERE clause.
     * 
     * @param relation the relation to add.
     */
    public void and(Relation relation)
    {
        if ((relation != null) && relation.isKey())
        {
            if (relation.operator().equals(RelationType.EQ))
                keys.add(relation.getValue());
            else if ((relation.operator().equals(RelationType.GT) || relation.operator().equals(RelationType.GTE)))
                startKey = relation.getValue();
            else if ((relation.operator().equals(RelationType.LT) || relation.operator().equals(RelationType.LTE)))
                finishKey = relation.getValue();
            
        }
        else
            columns.add(relation);
    }
    
    public List<Relation> getColumnRelations()
    {
        return columns;
    }
    
    public boolean isKeyRange()
    {
        return startKey != null;
    }
    
    public boolean isKeyList()
    {
        return !isKeyRange();
    }
    
    public Term getStartKey()
    {
        return startKey;
    }
    
    public Term getFinishKey()
    {
        return finishKey;
    }
    
    public List<Term> getKeys()
    {
        return keys;
    }
}
