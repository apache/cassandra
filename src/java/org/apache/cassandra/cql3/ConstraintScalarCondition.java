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


import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedAsymmetricSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class ConstraintScalarCondition implements ConstraintCondition
{
    public final ColumnIdentifier param;
    public final Operator relationType;
    public final String term;

    public final static Serializer serializer = new Serializer();

    public final static class Raw
    {
        public final ColumnIdentifier param;
        public final Operator relationType;
        public final String term;

        public Raw(ColumnIdentifier param, Operator relationType, String term)
        {
            this.param = param;
            this.relationType = relationType;
            this.term = term;
        }

        public ConstraintScalarCondition prepare()
        {
            return new ConstraintScalarCondition(param, relationType, term);
        }
    }

    public ConstraintScalarCondition(ColumnIdentifier param, Operator relationType, String term)
    {
        this.param = param;
        this.relationType = relationType;
        this.term = term;
    }

    public void evaluate(Map<String, Term.Raw> columnValues, ColumnMetadata columnMetadata, TableMetadata tableMetadata)
    {
        BigDecimal columnValue;
        BigDecimal sizeConstraint;
        try
        {
            columnValue = new BigDecimal(columnValues.get(param.toString()).getText());
            sizeConstraint = new BigDecimal(term);
        }
        catch (final NumberFormatException exception)
        {
            throw new ConstraintViolationException(param + " and " + term + " need to be numbers.");
        }

        switch (relationType)
        {
            case EQ:
                if (!columnValue.equals(sizeConstraint))
                    throw new ConstraintViolationException(param + " value length should be exactly " + sizeConstraint);
                break;
            case NEQ:
                if (columnValue.equals(sizeConstraint))
                    throw new ConstraintViolationException(param + " value length different than " + sizeConstraint);
                break;
            case GT:
                if (columnValue.compareTo(sizeConstraint) <= 0)
                    throw new ConstraintViolationException(param + " value length should be larger than " + sizeConstraint);
                break;
            case LT:
                if (columnValue.compareTo(sizeConstraint) >= 0)
                    throw new ConstraintViolationException(param + " value length should be smaller than " + sizeConstraint);
                break;
            case GTE:
                if (columnValue.compareTo(sizeConstraint) < 0)
                    throw new ConstraintViolationException(param + " value length should be larger or equal than " + sizeConstraint);
                break;
            case LTE:
                if (columnValue.compareTo(sizeConstraint) > 0)
                    throw new ConstraintViolationException(param + " value length should be smaller or equal than " + sizeConstraint);
                break;
            default:
                throw new ConstraintViolationException("Invalid relation type: " + relationType);
        }
    }

    public void validate(Map<String, ColumnMetadata> columnMetadata, TableMetadata tableMetadata)
    {
        if (!(tableMetadata.getColumn(param).type instanceof org.apache.cassandra.db.marshal.NumberType))
            throw new ConstraintInvalidException(param + " is not a number");
    }

    @Override
    public String toString()
    {
        return String.format("%s %s %s", param, relationType, term);
    }

    @Override
    public IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition> getSerializer()
    {
        return serializer;
    }

    public static class Serializer implements IVersionedAsymmetricSerializer<ConstraintCondition, ConstraintCondition>
    {
        @Override
        public void serialize(ConstraintCondition constraintCondition, DataOutputPlus out, int version) throws IOException
        {
            ConstraintScalarCondition condition = (ConstraintScalarCondition) constraintCondition;
            out.writeUTF(condition.param.toString());
            out.writeUTF(condition.relationType.toString());
            out.writeUTF(condition.term);
        }

        @Override
        public ConstraintCondition deserialize(DataInputPlus in, int version) throws IOException
        {
            ColumnIdentifier param = new ColumnIdentifier(in.readUTF(), true);
            Operator relationType = Operator.valueOf(in.readUTF());
            return new ConstraintScalarCondition(param, relationType, in.readUTF());
        }

        @Override
        public long serializedSize(ConstraintCondition constraintCondition, int version)
        {
            ConstraintScalarCondition condition = (ConstraintScalarCondition) constraintCondition;
            return TypeSizes.sizeof(condition.term)
                   + TypeSizes.sizeof(condition.relationType.toString())
                   + TypeSizes.sizeof(condition.param.toString());
        }
    }
}
