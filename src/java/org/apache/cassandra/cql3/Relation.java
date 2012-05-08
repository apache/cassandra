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
package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
public class Relation
{
    private final ColumnIdentifier entity;
    private final Type relationType;
    private final Term value;
    private final List<Term> inValues;
    public final boolean onToken;

    public static enum Type
    {
        EQ, LT, LTE, GTE, GT, IN;

        public static Type forString(String s)
        {
            if (s.equals("="))
                return EQ;
            else if (s.equals("<"))
                return LT;
            else if (s.equals("<="))
                return LTE;
            else if (s.equals(">="))
                return GTE;
            else if (s.equals(">"))
                return GT;

            return null;
        }
    }

    private Relation(ColumnIdentifier entity, Type type, Term value, List<Term> inValues, boolean onToken)
    {
        this.entity = entity;
        this.relationType = type;
        this.value = value;
        this.inValues = inValues;
        this.onToken = onToken;
    }

    /**
     * Creates a new relation.
     *
     * @param entity the kind of relation this is; what the term is being compared to.
     * @param type the type that describes how this entity relates to the value.
     * @param value the value being compared.
     */
    public Relation(ColumnIdentifier entity, String type, Term value)
    {
        this(entity, Type.forString(type), value, null, false);
    }

    public Relation(ColumnIdentifier entity, String type, Term value, boolean onToken)
    {
        this(entity, Type.forString(type), value, null, onToken);
    }

    public static Relation createInRelation(ColumnIdentifier entity)
    {
        return new Relation(entity, Type.IN, null, new ArrayList<Term>(), false);
    }

    public Type operator()
    {
        return relationType;
    }

    public ColumnIdentifier getEntity()
    {
        return entity;
    }

    public Term getValue()
    {
        assert relationType != Type.IN;
        return value;
    }

    public List<Term> getInValues()
    {
        assert relationType == Type.IN;
        return inValues;
    }

    public void addInValue(Term t)
    {
        inValues.add(t);
    }

    @Override
    public String toString()
    {
        if (relationType == Type.IN)
            return String.format("%s IN %s", entity, inValues);
        else
            return String.format("%s %s %s", entity, relationType, value);
    }
}
