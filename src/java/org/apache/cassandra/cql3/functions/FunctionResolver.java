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
package org.apache.cassandra.cql3.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.AbstractMarker;
import org.apache.cassandra.cql3.AssignmentTestable;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.UserFunctions;

import static java.util.stream.Collectors.joining;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public final class FunctionResolver
{
    private FunctionResolver()
    {
    }

    public static ColumnSpecification makeArgSpec(String receiverKeyspace, String receiverTable, Function fun, int i)
    {
        return new ColumnSpecification(receiverKeyspace,
                                       receiverTable,
                                       new ColumnIdentifier("arg" + i + '(' + fun.name().toString().toLowerCase() + ')', true),
                                       fun.argTypes().get(i));
    }

    /**
     * @param keyspace the current keyspace
     * @param name the name of the function
     * @param providedArgs the arguments provided for the function call
     * @param receiverKeyspace the receiver's keyspace
     * @param receiverTable the receiver's table
     * @param receiverType if the receiver type is known (during inserts, for example), this should be the type of
     *                     the receiver
     */
    @Nullable
    public static Function get(String keyspace,
                               FunctionName name,
                               List<? extends AssignmentTestable> providedArgs,
                               String receiverKeyspace,
                               String receiverTable,
                               AbstractType<?> receiverType)
    throws InvalidRequestException
    {
        return get(keyspace, name, providedArgs, receiverKeyspace, receiverTable, receiverType, UserFunctions.none());
    }

    /**
     * @param keyspace the current keyspace
     * @param name the name of the function
     * @param providedArgs the arguments provided for the function call
     * @param receiverKeyspace the receiver's keyspace
     * @param receiverTable the receiver's table
     * @param receiverType if the receiver type is known (during inserts, for example), this should be the type of
     *                     the receiver
     * @param functions a set of user functions that is not yet available in the schema, used during startup when those
     *                  functions might not be yet available
     */
    @Nullable
    public static Function get(String keyspace,
                               FunctionName name,
                               List<? extends AssignmentTestable> providedArgs,
                               String receiverKeyspace,
                               String receiverTable,
                               AbstractType<?> receiverType,
                               UserFunctions functions)
    throws InvalidRequestException
    {
        Collection<Function> candidates = collectCandidates(keyspace, name, receiverKeyspace, receiverTable, providedArgs, receiverType, functions);

        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.iterator().next();
            validateTypes(keyspace, fun, providedArgs, receiverKeyspace, receiverTable);
            return fun;
        }

        return pickBestMatch(keyspace, name, providedArgs, receiverKeyspace, receiverTable, receiverType, candidates);
    }

    private static Collection<Function> collectCandidates(String keyspace,
                                                          FunctionName name,
                                                          String receiverKeyspace,
                                                          String receiverTable,
                                                          List<? extends AssignmentTestable> providedArgs,
                                                          AbstractType<?> receiverType,
                                                          UserFunctions functions)
    {
        Collection<Function> candidates = new ArrayList<>();

        if (name.hasKeyspace())
        {
            // function name is fully qualified (keyspace + name)
            candidates.addAll(functions.get(name));
            candidates.addAll(Schema.instance.getUserFunctions(name));
            candidates.addAll(NativeFunctions.instance.getFunctions(name));
            candidates.addAll(NativeFunctions.instance.getFactories(name).stream()
                                            .map(f -> f.getOrCreateFunction(providedArgs, receiverType, receiverKeyspace, receiverTable))
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList()));
        }
        else
        {
            // function name is not fully qualified
            // add 'current keyspace' candidates
            FunctionName userName = new FunctionName(keyspace, name.name);
            candidates.addAll(functions.get(userName));
            candidates.addAll(Schema.instance.getUserFunctions(userName));
            // add 'SYSTEM' (native) candidates
            FunctionName nativeName = name.asNativeFunction();
            candidates.addAll(NativeFunctions.instance.getFunctions(nativeName));
            candidates.addAll(NativeFunctions.instance.getFactories(nativeName).stream()
                                            .map(f -> f.getOrCreateFunction(providedArgs, receiverType, receiverKeyspace, receiverTable))
                                            .filter(Objects::nonNull)
                                            .collect(Collectors.toList()));
        }

        return candidates;
    }

    private static Function pickBestMatch(String keyspace,
                                          FunctionName name,
                                          List<? extends AssignmentTestable> providedArgs,
                                          String receiverKeyspace,
                                          String receiverTable,
                                          AbstractType<?> receiverType,
                                          Collection<Function> candidates)
    {
        List<Function> compatibles = null;
        for (Function toTest : candidates)
        {
            if (matchReturnType(toTest, receiverType))
            {
                AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKeyspace, receiverTable);
                switch (r)
                {
                    case EXACT_MATCH:
                        // We always favor exact matches
                        return toTest;
                    case WEAKLY_ASSIGNABLE:
                        if (compatibles == null)
                            compatibles = new ArrayList<>();
                        compatibles.add(toTest);
                        break;
                }
            }
        }

        if (compatibles == null)
        {
            if (OperationFcts.isOperation(name))
                throw invalidRequest("the '%s' operation is not supported between %s and %s",
                                     OperationFcts.getOperator(name), providedArgs.get(0), providedArgs.get(1));

            throw invalidRequest("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                 name, format(candidates));
        }

        if (compatibles.size() > 1)
        {
            if (OperationFcts.isOperation(name))
            {
                if (receiverType != null && !containsMarkers(providedArgs))
                {
                    for (Function toTest : compatibles)
                    {
                        List<AbstractType<?>> argTypes = toTest.argTypes();
                        if (receiverType.equals(argTypes.get(0)) && receiverType.equals(argTypes.get(1)))
                            return toTest;
                    }
                }
                throw invalidRequest("Ambiguous '%s' operation with args %s and %s: use type hint to disambiguate, example '(int) ?'",
                                     OperationFcts.getOperator(name), providedArgs.get(0), providedArgs.get(1));
            }

            if (OperationFcts.isNegation(name))
                throw invalidRequest("Ambiguous negation: use type casts to disambiguate");

            throw invalidRequest("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                                 name, format(compatibles));
        }

        return compatibles.get(0);
    }

    /**
     * Checks if at least one of the specified arguments is a marker.
     *
     * @param args the arguments to check
     * @return {@code true} if if at least one of the specified arguments is a marker, {@code false} otherwise
     */
    private static boolean containsMarkers(List<? extends AssignmentTestable> args)
    {
        return args.stream().anyMatch(AbstractMarker.Raw.class::isInstance);
    }

    /**
     * Checks that the return type of the specified function can be assigned to the specified receiver.
     *
     * @param fun the function to check
     * @param receiverType the receiver type
     * @return {@code true} if the return type of the specified function can be assigned to the specified receiver,
     * {@code false} otherwise.
     */
    private static boolean matchReturnType(Function fun, AbstractType<?> receiverType)
    {
        return receiverType == null || fun.returnType().testAssignment(receiverType.udfType()).isAssignable();
    }

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignmentTestable> providedArgs,
                                      String receiverKeyspace,
                                      String receiverTable)
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw invalidRequest("Invalid number of arguments in call to function %s: %d required but %d provided",
                                 fun.name(), fun.argTypes().size(), providedArgs.size());

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKeyspace, receiverTable, fun, i);
            if (!provided.testAssignment(keyspace, expected).isAssignable())
                throw invalidRequest("Type error: %s cannot be passed as argument %d of function %s of type %s",
                                     provided, i, fun.name(), expected.type.asCQL3Type());
        }
    }

    private static AssignmentTestable.TestResult matchAguments(String keyspace,
                                                               Function fun,
                                                               List<? extends AssignmentTestable> providedArgs,
                                                               String receiverKeyspace,
                                                               String receiverTable)
    {
        if (providedArgs.size() != fun.argTypes().size())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // It's an exact match if all are exact match, but is not assignable as soon as any is not assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);
            if (provided == null)
            {
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            ColumnSpecification expected = makeArgSpec(receiverKeyspace, receiverTable, fun, i);
            AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
            if (argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static String format(Collection<Function> funs)
    {
        return funs.stream().map(Function::toString).collect(joining(", "));
    }
}
