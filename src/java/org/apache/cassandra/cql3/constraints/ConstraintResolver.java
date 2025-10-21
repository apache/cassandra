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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.constraints.SatisfiabilityChecker.UnaryFunctionSatisfiabilityChecker;
import org.apache.cassandra.utils.LocalizeString;

public class ConstraintResolver
{
    private static final Logger logger = LoggerFactory.getLogger(ConstraintResolver.class);

    @VisibleForTesting
    public static ConstraintProvider customConstraintProvider = ServiceLoader.load(ConstraintProvider.class).findFirst().orElse(null);

    static
    {
        if (customConstraintProvider != null)
            logger.info("Found custom constraint provider {}", customConstraintProvider.getClass().getName());
    }

    public enum UnaryFunctions implements UnaryFunctionSatisfiabilityChecker
    {
        NOT_NULL(NotNullConstraint::new),
        JSON(JsonConstraint::new);

        public final Function<List<String>, ConstraintFunction> functionCreator;

        UnaryFunctions(Function<List<String>, ConstraintFunction> functionCreator)
        {
            this.functionCreator = functionCreator;
        }
    }

    public enum Functions
    {
        LENGTH(LengthConstraint::new),
        OCTET_LENGTH(OctetLengthConstraint::new),
        REGEXP(RegexpConstraint::new);

        public final Function<List<String>, ConstraintFunction> functionCreator;

        Functions(Function<List<String>, ConstraintFunction> functionCreator)
        {
            this.functionCreator = functionCreator;
        }
    }

    /**
     * Returns implementation of function constraint. First, it iterates over custom functions, when not found,
     * then it will look into built-in ones. If it is not found in either, throws an exception.
     *
     * @param functionName name of function of get an instance of a constraint of
     * @param arguments    arguments for constraint
     * @return new instance of constraint for given name
     * @throws InvalidConstraintDefinitionException in case constraint can not be resolved.
     */
    public static ConstraintFunction getConstraintFunction(String functionName, List<String> arguments)
    {
        if (customConstraintProvider != null)
        {
            Optional<ConstraintFunction> maybeConstraint = customConstraintProvider.getConstraintFunction(functionName, arguments);
            if (maybeConstraint.isPresent())
                return maybeConstraint.get();
        }

        return ConstraintResolver.getEnum(Functions.class, functionName)
                                 .map(c -> c.functionCreator.apply(arguments))
                                 .orElseThrow(() -> new InvalidConstraintDefinitionException("Unrecognized constraint function: " + functionName));
    }

    /**
     * Returns implementation of unary function constraint. First, it iterates over built-in functions, when not found,
     * then it will look into custom constraint provider, if any. If custom provider is not set or if it is not found
     * there either, throws an exception.
     *
     * @param functionName name of function of get an instance of a constraint of
     * @param arguments    arguments for constraint
     * @return new instance of constraint for given name
     * @throws InvalidConstraintDefinitionException in case constraint can not be resolved.
     */
    public static ConstraintFunction getUnaryConstraintFunction(String functionName, List<String> arguments)
    {
        if (customConstraintProvider != null)
        {
            Optional<UnaryConstraintFunction> maybeConstraint = customConstraintProvider.getUnaryConstraint(functionName, arguments);
            if (maybeConstraint.isPresent())
                return maybeConstraint.get();
        }

        return ConstraintResolver.getEnum(UnaryFunctions.class, functionName)
                                 .map(c -> c.functionCreator.apply(arguments))
                                 .orElseThrow(() -> new InvalidConstraintDefinitionException("Unrecognized constraint function: " + functionName));
    }

    public static SatisfiabilityChecker[] getConstraintFunctionSatisfiabilityCheckers()
    {
        List<SatisfiabilityChecker> checkers = new ArrayList<>(Arrays.asList(FunctionColumnConstraint.getSatisfiabilityCheckers()));

        if (customConstraintProvider != null)
        {
            List<? extends SatisfiabilityChecker> checkersCustom = customConstraintProvider.getConstraintFunctionSatisfiabilityCheckers();
            if (checkersCustom != null)
                checkers.addAll(checkersCustom);
        }

        return checkers.toArray(new SatisfiabilityChecker[checkers.size()]);
    }

    public static SatisfiabilityChecker[] getUnarySatisfiabilityCheckers()
    {
        List<SatisfiabilityChecker> checkers = new ArrayList<>(Arrays.asList(UnaryFunctions.values()));

        if (customConstraintProvider != null)
        {
            List<? extends SatisfiabilityChecker> checkersCustom = customConstraintProvider.getUnaryConstraintSatisfiabilityCheckers();
            if (checkersCustom != null)
                checkers.addAll(checkersCustom);
        }

        return checkers.toArray(new SatisfiabilityChecker[checkers.size()]);
    }

    public static <T extends Enum<T>> Optional<T> getEnum(Class<T> enumClass, String functionName)
    {
        try
        {
            return Optional.of(Enum.valueOf(enumClass, LocalizeString.toUpperCaseLocalized(functionName)));
        }
        catch (IllegalArgumentException e)
        {
            return Optional.empty();
        }
    }
}
