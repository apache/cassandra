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


import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * WhereClauses encapsulate all of the predicates of a SELECT query.
 *
 */
public class WhereClause
{
    // added to either by the parser, e.g. from an IN clause, or by extractKeysFromColumns
    private Set<Term> keys = new LinkedHashSet<Term>();
    private Term startKey, finishKey;
    private List<Relation> columns = new ArrayList<Relation>();
    private boolean includeStartKey = false, includeFinishKey = false, multiKey = false;
    // set by extractKeysFromColumns
    private String keyAlias = null;

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
            if (relation.operator() == RelationType.EQ)
                keys.add(relation.getValue());
            else if ((relation.operator() == RelationType.GT) || (relation.operator() == RelationType.GTE))
            {
                startKey = relation.getValue();
                includeStartKey = relation.operator() == RelationType.GTE;
            }
            else if ((relation.operator() == RelationType.LT) || (relation.operator() == RelationType.LTE))
            {
                finishKey = relation.getValue();
                includeFinishKey = relation.operator() == RelationType.LTE;
            }
            
        }
        else
            columns.add(relation);
    }

    /**
     * The same as KEY = <key> to avoid using Relation object
     * @param key key to include into clause
     */
    public void andKeyEquals(Term key)
    {
        keys.add(key);
    }

    public List<Relation> getColumnRelations()
    {
        return columns;
    }
    
    public boolean isKeyRange()
    {
        return startKey != null;
    }
    
    public Term getStartKey()
    {
        return startKey;
    }
    
    public Term getFinishKey()
    {
        return finishKey;
    }
    
    public Set<Term> getKeys()
    {
        return keys;
    }

    public boolean includeStartKey()
    {
        return includeStartKey;
    }

    public boolean includeFinishKey()
    {
        return includeFinishKey;
    }

    public void setKeyAlias(String alias)
    {
        keyAlias = alias.toUpperCase();
    }

    public boolean isMultiKey() {
        return multiKey;
    }

    public void setMultiKey(boolean multiKey)
    {
        this.multiKey = multiKey;
    }

    public String getKeyAlias()
    {
        // TODO fix special casing here, key alias should always be set post-extract
        // key alias as not related to keys in here, it can be unset when we have a query like
        // SELECT * FROM <CF> WHERE key = 1 and col > 2 and col < 3;
        // it will be always set when statement looks like this
        // SELECT * FROM <CF> WHERE <key> IN (.., .., ..);
        // key is NULL when KEY keyword is used or when key alias given by user was not recognized
        // validateKeyAlias will throw an exception for us in that case
        return keyAlias == null ? QueryProcessor.DEFAULT_KEY_NAME : keyAlias;
    }

    public void extractKeysFromColumns(CFMetaData cfm)
    {
        ByteBuffer realKeyAlias = cfm.getKeyName();

        if (!keys.isEmpty())
            return; // we already have key(s) set

        Iterator<Relation> iter = columns.iterator();

        while (iter.hasNext())
        {
            Relation relation = iter.next();

            ByteBuffer name = ByteBufferUtil.bytes(relation.getEntity().getText());

            if (name.equals(realKeyAlias))
            {
                // setting found key as an alias
                keyAlias = relation.getEntity().getText().toUpperCase();
                keys.add(relation.getValue()); // add a key value to the keys list
                iter.remove(); // removing it from the columns
                break;
            }
        }
    }
}
