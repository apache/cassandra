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

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.schema.ColumnMetadata;

/**
 * Common class for the conditions that a CQL Constraint needs to implement to be integrated in the
 * CQL Constraints framework, with T as a constraint serializer.
 */
public abstract class ColumnConstraint<T>
{
    protected final ColumnIdentifier columnName;

    public ColumnConstraint(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
    }

    // Enum containing all the possible constraint serializers to help with serialization/deserialization
    // of constraints.
    public enum ConstraintType
    {
        // The order of that enum matters!!
        // We are serializing its enum position instead of its name.
        // Changing this enum would affect how that int is interpreted when deserializing.
        COMPOSED(ColumnConstraints.serializer),
        FUNCTION(FunctionColumnConstraint.serializer),
        SCALAR(ScalarColumnConstraint.serializer),
        UNARY_FUNCTION(UnaryFunctionColumnConstraint.serializer);

        private final MetadataSerializer<?> serializer;

        ConstraintType(MetadataSerializer<?> serializer)
        {
            this.serializer = serializer;
        }

        public static MetadataSerializer<?> getSerializer(int i)
        {
            return ConstraintType.values()[i].serializer;
        }
    }

    public abstract MetadataSerializer<T> serializer();

    public abstract void appendCqlTo(CqlBuilder builder);

    /**
     * Method that evaluates the condition. It can either succeed or throw a {@link ConstraintViolationException}.
     *
     * @param valueType value type of the column value under test
     * @param columnValue Column value to be evaluated at write time
     */
    public void evaluate(AbstractType<?> valueType, ByteBuffer columnValue) throws ConstraintViolationException
    {
        if (columnValue.capacity() == 0)
            throw new ConstraintViolationException("Column value does not satisfy value constraint for column '" + columnName + "' as it is null.");

        internalEvaluate(valueType, columnValue);
    }

    /**
     * Internal evaluation method, by default called from {@link ColumnConstraint#evaluate(AbstractType, ByteBuffer)}.
     * {@code columnValue} is by default guaranteed to not represent CQL value of 'null'.
     */
    protected abstract void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue);

    /**
     * Method to validate the condition. This method is called when creating constraint via CQL.
     * A {@link InvalidConstraintDefinitionException} is thrown for invalid consrtaint definition.
     *
     * @param columnMetadata Metadata of the column in which the constraint is defined.
     */
    public abstract void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException;

    /**
     * Method to get the Constraint serializer
     *
     * @return the Constraint type serializer
     */
    public abstract ConstraintType getConstraintType();
}
