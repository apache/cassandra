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
        super(null);
        this.constraints = constraints;
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
        for (ColumnConstraint c : constraints)
        {
            if (c != ColumnConstraints.NO_OP)
                return true;
        }
        return false;
    }

    @Override
    public void validate(ColumnMetadata columnMetadata) throws InvalidConstraintDefinitionException
    {
        if (!columnMetadata.type.isConstrainable())
            throw new InvalidConstraintDefinitionException("Constraint cannot be defined on the column "
                                                           + columnMetadata.name + " of type " + columnMetadata.type.asCQL3Type()
                                                           + " for the table " + columnMetadata.ksName + "." + columnMetadata.cfName);

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

        public ColumnConstraints prepare()
        {
            if (constraints.isEmpty())
                return NO_OP;
            return new ColumnConstraints(constraints);
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
            {
                int serializerPosition = in.readShort();
                ColumnConstraint<?> constraint = (ColumnConstraint<?>) ConstraintType
                                                                       .getSerializer(serializerPosition)
                                                                       .deserialize(in, version);
                columnConstraints.add(constraint);
            }
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
