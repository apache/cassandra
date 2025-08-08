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

import java.util.List;
import java.util.Optional;

/**
 * Users implementing this interface and integrating it with SPI (putting JAR on a class path and
 * adding it to META-INF/services/ConstraintProvider) will enrich Cassandra with their custom constraints.
 */
public interface ConstraintProvider
{
    /**
     * Tries to instantiate {@link UnaryConstraintFunction} with given arguments.
     * <p>
     * An implementation of this method should always return new object for each method call. Do not
     * cache constraint instances and do not return them! Create a new instance every time. Do not re-use it.
     *
     * @param functionName name of function
     * @param arguments    arguments to the function
     * @return unary constraint function when possible to create with this provider, empty optional otherwise.
     */
    Optional<UnaryConstraintFunction> getUnaryConstraint(String functionName, List<String> arguments);

    /**
     * Tries to instantiate {@link ConstraintFunction} with given arguments.
     * <p>
     * An implementation of this method should always return new object for each method call. Do not
     * cache constraint instances and do not return them! Create a new instance every time. Do not re-use it.
     *
     * @param functionName name of function
     * @param arguments    arguments to the function
     * @return constraint function when possible to create with this provider, empty optional otherwise.
     */
    Optional<ConstraintFunction> getConstraintFunction(String functionName, List<String> arguments);

    /**
     * @return list of satisfiability checkers for all unary constraints this provider is responsible for
     */
    List<? extends SatisfiabilityChecker> getUnaryConstraintSatisfiabilityCheckers();

    /**
     * @return list of satisfiability checkers for all function constraints this provider is responsible for
     */
    List<? extends SatisfiabilityChecker> getConstraintFunctionSatisfiabilityCheckers();
}
