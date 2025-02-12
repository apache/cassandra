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
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;

import static java.lang.String.format;
import static org.apache.cassandra.cql3.Operator.EQ;
import static org.apache.cassandra.cql3.Operator.GT;
import static org.apache.cassandra.cql3.Operator.GTE;
import static org.apache.cassandra.cql3.Operator.LT;
import static org.apache.cassandra.cql3.Operator.LTE;
import static org.apache.cassandra.cql3.Operator.NEQ;
import static org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType.FUNCTION;

public abstract class AbstractFunctionSatisfiabilityChecker<CONSTRAINT_TYPE extends AbstractFunctionConstraint<CONSTRAINT_TYPE>>
{
    /**
     * Performs check if constraints are satisfiable or not.
     *
     * @param functionName   name of function
     * @param constraints    list of constraints to set
     * @param columnMetadata metadata of a column.
     */
    public void check(String functionName, List<ColumnConstraint<?>> constraints, ColumnMetadata columnMetadata)
    {
        Pair<List<CONSTRAINT_TYPE>, List<CONSTRAINT_TYPE>> filteredConstraints = categorizeConstraints(constraints, functionName);

        if (filteredConstraints.left.isEmpty())
            return;

        checkNumberOfConstraints(columnMetadata, filteredConstraints);
        checkSupportedOperators(filteredConstraints.left, functionName);
        ensureSatisfiability(columnMetadata, functionName, filteredConstraints.left);
    }

    /**
     * Categorizes given constraints into two lists. The first list, the left one in Pair, contains all
     * constraints of implementation-specific {@link org.apache.cassandra.cql3.constraints.ColumnConstraint.ConstraintType}.
     * The second list, the right one in Pair, contains all constraints of such constraint type which do have "not equal" operator.
     *
     * @param constraints  constraints to categorize
     * @param functionName name of function
     * @return pair of categorized constraints
     */
    abstract Pair<List<CONSTRAINT_TYPE>, List<CONSTRAINT_TYPE>> categorizeConstraints(List<ColumnConstraint<?>> constraints, String functionName);

    private void checkSupportedOperators(List<CONSTRAINT_TYPE> allConstraints, String functionName)
    {
        for (CONSTRAINT_TYPE constraint : allConstraints)
        {
            if (!constraint.getSupportedOperators().contains(constraint.relationType()))
                throw new InvalidConstraintDefinitionException(format("%s constraint of relation '%s' is not supported. Only these are: %s",
                                                                      functionName,
                                                                      constraint.relationType(),
                                                                      constraint.getSupportedOperators()));
        }
    }

    /**
     * Checks if there are no duplicate constraints having same operator.
     *
     * @param columnMetadata      medata of a column
     * @param filteredConstraints pair of all constraints and all constraints having not-equal operator
     */
    private void checkNumberOfConstraints(ColumnMetadata columnMetadata, Pair<List<CONSTRAINT_TYPE>, List<CONSTRAINT_TYPE>> filteredConstraints)
    {
        List<? extends AbstractFunctionConstraint<CONSTRAINT_TYPE>> allConstraints = filteredConstraints.left;
        List<? extends AbstractFunctionConstraint<CONSTRAINT_TYPE>> notEqualConstraints = filteredConstraints.right;

        if ((allConstraints.size() - notEqualConstraints.size() > 2))
        {
            throw new InvalidConstraintDefinitionException(format("There can not be more than 2 constraints (not including non-equal relations) on a column '%s' but you have specified %s",
                                                                  columnMetadata.name,
                                                                  allConstraints.size()));
        }

        if (notEqualConstraints.size() > 1)
        {
            Set<String> uniqueTerms = new TreeSet<>();
            for (AbstractFunctionConstraint<CONSTRAINT_TYPE> notEqual : notEqualConstraints)
            {
                if (!uniqueTerms.add(notEqual.term()))
                    throw new InvalidConstraintDefinitionException(format("There are duplicate constraint definitions on column '%s': %s",
                                                                          columnMetadata.name,
                                                                          notEqual));
            }
        }
    }

    private void ensureSatisfiability(ColumnMetadata columnMetadata,
                                      String constraintName,
                                      List<CONSTRAINT_TYPE> allConstraints)
    {
        if (allConstraints.size() != 2)
            return;

        Operator firstRelation = allConstraints.get(0).relationType();
        String firstTerm = allConstraints.get(0).term();
        Operator secondRelation = allConstraints.get(1).relationType();
        String secondTerm = allConstraints.get(1).term();

        if ((firstRelation == GT && secondRelation == GTE) ||
            (firstRelation == GTE && secondRelation == GT) ||
            (firstRelation == LT && secondRelation == LTE) ||
            (firstRelation == LTE && secondRelation == LT) ||
            (firstRelation == EQ || secondRelation == EQ))
        {
            throw new InvalidConstraintDefinitionException(format("Constraints combination of %s is not supported: %s %s %s, %s %s %s",
                                                                  constraintName,
                                                                  columnMetadata.name,
                                                                  firstRelation,
                                                                  firstTerm,
                                                                  columnMetadata.name,
                                                                  secondRelation,
                                                                  secondTerm));
        }
        else if (firstRelation == NEQ && secondRelation == NEQ)
        {
            if (firstTerm.equals(secondTerm))
                throw new InvalidConstraintDefinitionException(format("There are duplicate constraint definitions on column '%s'.", columnMetadata.name));
        }
        else
        {
            ByteBuffer firstTermBuffer = columnMetadata.type.fromString(firstTerm);
            ByteBuffer secondTermBuffer = columnMetadata.type.fromString(secondTerm);

            boolean firstSatisfaction = firstRelation.isSatisfiedBy(columnMetadata.type, secondTermBuffer, firstTermBuffer);
            boolean secondSatisfaction = secondRelation.isSatisfiedBy(columnMetadata.type, firstTermBuffer, secondTermBuffer);

            if (!firstSatisfaction || !secondSatisfaction)
                throw new InvalidConstraintDefinitionException(format("Constraints of %s are not satisfiable: %s %s %s, %s %s %s",
                                                                      constraintName,
                                                                      columnMetadata.name,
                                                                      firstRelation,
                                                                      firstTerm,
                                                                      columnMetadata.name,
                                                                      secondRelation,
                                                                      secondTerm));
        }
    }

    public static final AbstractFunctionSatisfiabilityChecker<ScalarColumnConstraint> SCALAR_SATISFIABILITY_CHECKER = new AbstractFunctionSatisfiabilityChecker<>()
    {
        @Override
        public Pair<List<ScalarColumnConstraint>, List<ScalarColumnConstraint>> categorizeConstraints(List<ColumnConstraint<?>> constraints, String functionName)
        {
            List<ScalarColumnConstraint> scalars = new LinkedList<>();
            List<ScalarColumnConstraint> notEqualScalars = new LinkedList<>();

            for (ColumnConstraint<?> columnConstraint : constraints)
            {
                if (columnConstraint.getConstraintType() == ColumnConstraint.ConstraintType.SCALAR)
                {
                    ScalarColumnConstraint scalarColumnConstraint = (ScalarColumnConstraint) columnConstraint;
                    scalars.add(scalarColumnConstraint);
                    if (scalarColumnConstraint.relationType() == NEQ)
                        notEqualScalars.add(scalarColumnConstraint);
                }
            }

            return Pair.create(scalars, notEqualScalars);
        }
    };

    public static final AbstractFunctionSatisfiabilityChecker<FunctionColumnConstraint> FUNCTION_SATISFIABILITY_CHECKER = new AbstractFunctionSatisfiabilityChecker<>()
    {
        @Override
        public Pair<List<FunctionColumnConstraint>, List<FunctionColumnConstraint>> categorizeConstraints(List<ColumnConstraint<?>> constraints, String functionName)
        {
            List<FunctionColumnConstraint> funnctionColumnConstraints = new LinkedList<>();
            List<FunctionColumnConstraint> notEqualConstraints = new LinkedList<>();

            for (ColumnConstraint<?> columnConstraint : constraints)
            {
                if (columnConstraint.getConstraintType() != FUNCTION)
                    continue;

                FunctionColumnConstraint functionColumnConstraint = (FunctionColumnConstraint) columnConstraint;

                ConstraintFunction function = functionColumnConstraint.function();

                if (!function.name.equals(functionName))
                    continue;

                funnctionColumnConstraints.add(functionColumnConstraint);
                if (functionColumnConstraint.relationType() == NEQ)
                    notEqualConstraints.add(functionColumnConstraint);
            }

            return Pair.create(funnctionColumnConstraints, notEqualConstraints);
        }
    };
}
