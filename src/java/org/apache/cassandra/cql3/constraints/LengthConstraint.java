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

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class LengthConstraint extends ConstraintFunction
{
    private static final String NAME = "LENGTH";
    private static final AbstractType<?>[] SUPPORTED_TYPES = new AbstractType[]{ BytesType.instance, UTF8Type.instance, AsciiType.instance };

    public LengthConstraint(ColumnIdentifier columnName)
    {
        super(columnName, NAME);
    }

    @Override
    public void internalEvaluate(AbstractType<?> valueType, Operator relationType, String term, ByteBuffer columnValue)
    {
        int valueLength = getValueLength(columnValue, valueType);
        int sizeConstraint = Integer.parseInt(term);

        ByteBuffer leftOperand = ByteBufferUtil.bytes(valueLength);
        ByteBuffer rightOperand = ByteBufferUtil.bytes(sizeConstraint);

        if (!relationType.isSatisfiedBy(Int32Type.instance, leftOperand, rightOperand))
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "'. "
                                                   + "It has a length of " + valueLength + " and it should be "
                                                   + relationType + ' ' + term);
    }

    @Override
    public void validate(ColumnMetadata columnMetadata)
    {
        boolean supported = false;
        AbstractType<?> unwrapped = columnMetadata.type.unwrap();
        for (AbstractType<?> supportedType : SUPPORTED_TYPES)
        {
            if (supportedType == unwrapped)
            {
                supported = true;
                break;
            }
        }

        if (!supported)
            throw invalidConstraintDefinitionException(columnMetadata.type);
    }

    @Override
    public Set<Operator> getSupportedOperators()
    {
        return DEFAULT_FUNCTION_OPERATORS;
    }

    private int getValueLength(ByteBuffer value, AbstractType<?> valueType)
    {
        if (valueType.getClass() == BytesType.class)
        {
            return value.remaining();
        }

        if (valueType.getClass() == AsciiType.class || valueType.getClass() == UTF8Type.class)
            return ((String) valueType.compose(value)).length();

        throw invalidConstraintDefinitionException(valueType);
    }

    private InvalidConstraintDefinitionException invalidConstraintDefinitionException(AbstractType<?> valueType)
    {
        throw new InvalidConstraintDefinitionException("Column type " + valueType.getClass() + " is not supported.");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof LengthConstraint))
            return false;

        LengthConstraint other = (LengthConstraint) o;

        return columnName.equals(other.columnName);
    }
}
