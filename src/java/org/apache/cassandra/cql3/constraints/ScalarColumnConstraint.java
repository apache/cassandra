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

package org.apache.cassandra.cql3.constraints;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class ScalarColumnConstraint extends ColumnConstraint<ScalarColumnConstraint>
{
    public final static Serializer serializer = new Serializer();

    private final Operator relationType;
    private final String term;

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

        public ScalarColumnConstraint prepare()
        {
            return new ScalarColumnConstraint(param, relationType, term);
        }
    }

    private ScalarColumnConstraint(ColumnIdentifier param, Operator relationType, String term)
    {
        super(param);
        this.relationType = relationType;
        this.term = term;
    }


    @Override
    protected void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue)
    {
        ByteBuffer value;
        try
        {
            value = valueType.fromString(term);
        }
        catch (NumberFormatException exception)
        {
            throw new ConstraintViolationException(columnName + " and " + term + " need to be numbers.");
        }

        if (!relationType.isSatisfiedBy(valueType, columnValue, value))
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "'. "
                                                   + "It should be " + columnName + " " + relationType + " " + term);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        if (!columnMetadata.type.isNumber())
            throw new InvalidConstraintDefinitionException("Column '" + columnName + "' is not a number type.");
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.SCALAR;
    }

    @Override
    public String toString()
    {
        return columnName + " " + relationType + " " + term;
    }

    @Override
    public MetadataSerializer<ScalarColumnConstraint> serializer()
    {
        return serializer;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder)
    {
        builder.append(toString());
    }

    private static class Serializer implements MetadataSerializer<ScalarColumnConstraint>
    {
        @Override
        public void serialize(ScalarColumnConstraint columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(columnConstraint.columnName.toCQLString());
            columnConstraint.relationType.writeTo(out);
            out.writeUTF(columnConstraint.term);
        }

        @Override
        public ScalarColumnConstraint deserialize(DataInputPlus in, Version version) throws IOException
        {
            ColumnIdentifier param = new ColumnIdentifier(in.readUTF(), true);
            Operator relationType = Operator.readFrom(in);
            return new ScalarColumnConstraint(param, relationType, in.readUTF());
        }

        @Override
        public long serializedSize(ScalarColumnConstraint columnConstraint, Version version)
        {
            return TypeSizes.sizeof(columnConstraint.term)
                   + Operator.serializedSize()
                   + TypeSizes.sizeof(columnConstraint.columnName.toString());
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ScalarColumnConstraint))
            return false;

        ScalarColumnConstraint other = (ScalarColumnConstraint) o;

        return columnName.equals(other.columnName)
               && relationType == other.relationType
               && term.equals(other.term);
    }
}
