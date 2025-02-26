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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.cql3.constraints.ColumnConstraints.DuplicatesChecker;
import org.apache.cassandra.cql3.constraints.ScalarColumnConstraint.ScalarColumnConstraintSatisfiabilityChecker;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;

import static java.lang.String.format;

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
        COMPOSED(ColumnConstraints.serializer, new DuplicatesChecker()),
        FUNCTION(FunctionColumnConstraint.serializer, FunctionColumnConstraint.getSatisfiabilityCheckers()),
        SCALAR(ScalarColumnConstraint.serializer, new ScalarColumnConstraintSatisfiabilityChecker()),
        UNARY_FUNCTION(UnaryFunctionColumnConstraint.serializer, UnaryFunctionColumnConstraint.Functions.values());

        private final MetadataSerializer<?> serializer;
        private final SatisfiabilityChecker[] satisfiabilityCheckers;

        ConstraintType(MetadataSerializer<?> serializer, SatisfiabilityChecker satisfiabilityChecker)
        {
            this(serializer, new SatisfiabilityChecker[]{ satisfiabilityChecker });
        }

        ConstraintType(MetadataSerializer<?> serializer, SatisfiabilityChecker[] satisfiabilityCheckers)
        {
            this.serializer = serializer;
            this.satisfiabilityCheckers = satisfiabilityCheckers;
        }

        public static MetadataSerializer<?> getSerializer(int i)
        {
            return ConstraintType.values()[i].serializer;
        }

        public static SatisfiabilityChecker[] getSatisfiabilityCheckers()
        {
            List<SatisfiabilityChecker> result = new ArrayList<>();
            for (ConstraintType constraintType : ConstraintType.values())
                result.addAll(Arrays.asList(constraintType.satisfiabilityCheckers));

            return result.toArray(new SatisfiabilityChecker[0]);
        }
    }

    public abstract String name();

    /**
     * Typically includes name of a constraint as in {@link #name()},
     * plus an operator of a function, if constraint is a function.
     * Full name serves as String which uniquely distinguishes two constraints even of same names for the purpose
     * of checking if there is a specific constraint used twice. A duplicit usage of a constraint is illegal.
     *
     * @return full name of a constraint, with an operator.
     */
    public String fullName()
    {
        return name();
    }

    public abstract MetadataSerializer<T> serializer();

    public abstract void appendCqlTo(CqlBuilder builder);

    public abstract boolean enablesDuplicateDefinitions(String name);

    /**
     * Method that evaluates the condition. It can either succeed or throw a {@link ConstraintViolationException}.
     *
     * @param valueType   value type of the column value under test
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


    /**
     * Tells what types of columns are supported by this constraint.
     * Returning empty list or null means that all types are supported.
     *
     * @return supported types for given constraint
     */
    public abstract List<AbstractType<?>> getSupportedTypes();

    protected void validateTypes(ColumnMetadata columnMetadata)
    {
        if (getSupportedTypes() == null || getSupportedTypes().isEmpty())
            return;

        if (!getSupportedTypes().contains(columnMetadata.type.unwrap()))
            throw new InvalidConstraintDefinitionException(format("Constraint '%s' can be used only for columns of type %s but it was %s",
                                                                  name(),
                                                                  getSupportedTypes(),
                                                                  columnMetadata.type.getClass()));
    }
}
