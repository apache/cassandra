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

import java.util.List;

/**
 * Relations encapsulate the relationship between an entity of some kind, and
 * a value (term). For example, <key> > "start" or "colname1" = "somevalue".
 *
 */
public class SingleColumnRelation extends Relation
{
    private final ColumnIdentifier.Raw entity;
    private final Term.Raw value;
    private final List<Term.Raw> inValues;
    public final boolean onToken;

    private SingleColumnRelation(ColumnIdentifier.Raw entity, Operator type, Term.Raw value, List<Term.Raw> inValues, boolean onToken)
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
    public SingleColumnRelation(ColumnIdentifier.Raw entity, Operator type, Term.Raw value)
    {
        this(entity, type, value, null, false);
    }

    public SingleColumnRelation(ColumnIdentifier.Raw entity, Operator type, Term.Raw value, boolean onToken)
    {
        this(entity, type, value, null, onToken);
    }

    public static SingleColumnRelation createInRelation(ColumnIdentifier.Raw entity, List<Term.Raw> inValues)
    {
        return new SingleColumnRelation(entity, Operator.IN, null, inValues, false);
    }

    public ColumnIdentifier.Raw getEntity()
    {
        return entity;
    }

    public Term.Raw getValue()
    {
        assert relationType != Operator.IN || value == null || value instanceof AbstractMarker.INRaw;
        return value;
    }

    public List<Term.Raw> getInValues()
    {
        assert relationType == Operator.IN;
        return inValues;
    }

    public boolean isMultiColumn()
    {
        return false;
    }

    public boolean isOnToken()
    {
        return onToken;
    }

    public SingleColumnRelation withNonStrictOperator()
    {
        switch (relationType)
        {
            case GT: return new SingleColumnRelation(entity, Operator.GTE, value);
            case LT:  return new SingleColumnRelation(entity, Operator.LTE, value);
            default: return this;
        }
    }

    @Override
    public String toString()
    {
        if (relationType == Operator.IN)
            return String.format("%s IN %s", entity, inValues);
        else if (onToken)
            return String.format("token(%s) %s %s", entity, relationType, value);
        else
            return String.format("%s %s %s", entity, relationType, value);
    }
}
