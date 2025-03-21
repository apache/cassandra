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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static java.lang.String.format;

// group of constraints for the column
public class ColumnConstraints extends ColumnConstraint<ColumnConstraints>
{
    public static final Serializer serializer = new Serializer();
    public static final ColumnConstraints NO_OP = new Noop();

    private final List<ColumnConstraint<?>> constraints;

    public ColumnConstraints(List<ColumnConstraint<?>> constraints)
    {
        this.constraints = constraints;
    }

    @Override
    public void setColumnName(ColumnIdentifier columnName)
    {
        this.columnName = columnName;
        for (ColumnConstraint<?> constraint : constraints)
            constraint.setColumnName(columnName);
    }

    @Override
    public String name()
    {
        return getConstraintType().name();
    }

    @Override
    public MetadataSerializer<ColumnConstraints> serializer()
    {
        return serializer;
    }

    @Override
    public void appendCqlTo(CqlBuilder builder)
    {
        for (ColumnConstraint<?> constraint : constraints)
            constraint.appendCqlTo(builder);
    }

    @Override
    public boolean enablesDuplicateDefinitions(String name)
    {
        return false;
    }

    @Override
    public void evaluate(AbstractType<?> valueType, ByteBuffer columnValue) throws ConstraintViolationException
    {
        for (ColumnConstraint<?> constraint : constraints)
            constraint.evaluate(valueType, columnValue);
    }

    @Override
    protected void internalEvaluate(AbstractType<?> valueType, ByteBuffer columnValue)
    {
        // nothing to evaluate here
    }

    public List<ColumnConstraint<?>> getConstraints()
    {
        return constraints;
    }

    public boolean isEmpty()
    {
        return constraints.isEmpty();
    }

    public int getSize()
    {
        return constraints.size();
    }

    // Checks if there is at least one constraint that will perform checks
    public boolean hasRelevantConstraints()
    {
        for (ColumnConstraint<?> c : constraints)
        {
            if (c != ColumnConstraints.NO_OP)
                return true;
        }
        return false;
    }

    public boolean containsNotNullConstraint()
    {
        for (ColumnConstraint<?> c : constraints)
        {
            if (c.toString().equals(NotNullConstraint.CQL_FUNCTION_NAME))
                return true;
        }

        return false;
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        if (!columnMetadata.type.isConstrainable())
        {
            throw new InvalidConstraintDefinitionException("Constraint cannot be defined on the column "
                                                           + columnMetadata.name + " of type " + columnMetadata.type.asCQL3Type()
                                                           + " for the table " + columnMetadata.ksName + '.' + columnMetadata.cfName + '.' +
                                                           (columnMetadata.type.isCollection() ? " When using collections, constraints can be used only of frozen collections." : ""));
        }

        // this will look at constraints as a whole,
        // checking if combinations of a particular constraint make sense (duplicities, satisfiability etc.).
        for (SatisfiabilityChecker satisfiabilityChecker : ConstraintType.getSatisfiabilityCheckers())
            satisfiabilityChecker.checkSatisfiability(constraints, columnMetadata);

        // this validation will check whether it makes sense to execute such constraint on a given column
        for (ColumnConstraint<?> constraint : constraints)
            constraint.validate(columnMetadata);
    }

    @Override
    public ConstraintType getConstraintType()
    {
        return ConstraintType.COMPOSED;
    }

    @Override
    public List<AbstractType<?>> getSupportedTypes()
    {
        return null;
    }

    public static class DuplicatesChecker implements SatisfiabilityChecker
    {
        @Override
        public void checkSatisfiability(List<ColumnConstraint<?>> constraints, ColumnMetadata columnMetadata)
        {
            Set<String> constraintNames = new TreeSet<>();
            List<String> duplicateConstraints = new ArrayList<>();

            for (ColumnConstraint<?> constraint : constraints)
            {
                String constraintFullName = constraint.fullName();
                String constraintName = constraint.name();

                if (!constraintNames.add(constraintFullName))
                {
                    if (!constraint.enablesDuplicateDefinitions(constraintName))
                        duplicateConstraints.add(constraintFullName);
                }
            }

            if (!duplicateConstraints.isEmpty())
                throw new InvalidConstraintDefinitionException(format("There are duplicate constraint definitions on column '%s': %s",
                                                                      columnMetadata.name,
                                                                      duplicateConstraints));
        }
    }

    private static class Noop extends ColumnConstraints
    {
        private Noop()
        {
            super(Collections.emptyList());
        }

        @Override
        public void validate(ColumnMetadata columnMetadata)
        {
            // Do nothing. It is always valid
        }

        @Override
        public String name()
        {
            return "NO_OP";
        }
    }

    public final static class Raw
    {
        private final List<ColumnConstraint<?>> constraints;

        public Raw(List<ColumnConstraint<?>> constraints)
        {
            this.constraints = constraints;
        }

        public Raw()
        {
            this.constraints = Collections.emptyList();
        }

        public ColumnConstraints prepare(ColumnIdentifier column)
        {
            if (constraints.isEmpty())
                return NO_OP;

            for (ColumnConstraint<?> constraint : constraints)
            {
                // We only check scalar constraints column name, as the rest of the constraints
                // imply the name from the column they are defined at
                if (constraint.getConstraintType() == ConstraintType.SCALAR)
                {
                    if (!column.equals(constraint.columnName))
                    {
                        throw new InvalidConstraintDefinitionException(format("Constraint %s was not specified on a column it operates on: %s but on: %s",
                                                                              constraint, column.toCQLString(), constraint.columnName));
                    }
                }
            }

            ColumnConstraints columnConstraints = new ColumnConstraints(constraints);
            columnConstraints.setColumnName(column);
            return columnConstraints;
        }
    }

    public static class Serializer implements MetadataSerializer<ColumnConstraints>
    {
        @Override
        public void serialize(ColumnConstraints columnConstraint, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(columnConstraint.getSize());
            for (ColumnConstraint constraint : columnConstraint.getConstraints())
            {
                // We serialize the serializer ordinal in the enum to save space
                out.writeShort(constraint.getConstraintType().ordinal());
                constraint.serializer().serialize(constraint, out, version);
            }
        }

        @Override
        public ColumnConstraints deserialize(DataInputPlus in, Version version) throws IOException
        {
            List<ColumnConstraint<?>> columnConstraints = new ArrayList<>();
            int numberOfConstraints = in.readInt();
            for (int i = 0; i < numberOfConstraints; i++)
                columnConstraints.add(deserializeConstraint(in, in.readShort(), version));

            // we are not setting column name here on purpose
            // that is deffered in ColumnMetadata's constructor,
            // we do not have the access to a column name here anyway
            return new ColumnConstraints(columnConstraints);
        }

        @Override
        public long serializedSize(ColumnConstraints columnConstraint, Version version)
        {
            long constraintsSize = TypeSizes.INT_SIZE;
            for (ColumnConstraint constraint : columnConstraint.getConstraints())
            {
                constraintsSize += TypeSizes.SHORT_SIZE;
                constraintsSize += constraint.serializer().serializedSize(constraint, version);
            }
            return constraintsSize;
        }

        @VisibleForTesting
        public ColumnConstraint<?> deserializeConstraint(DataInputPlus in, int serializerPosition, Version version) throws IOException
        {
            return (ColumnConstraint<?>) ConstraintType
                                         .getSerializer(serializerPosition)
                                         .deserialize(in, version);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof ColumnConstraints))
            return false;

        ColumnConstraints other = (ColumnConstraints) o;
        return Objects.equals(constraints, other.constraints);
    }
}
