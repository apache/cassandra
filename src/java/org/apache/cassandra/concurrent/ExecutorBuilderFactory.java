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

package org.apache.cassandra.concurrent;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Entry point for configuring and creating new executors.
 *
 * Supports quick and easy construction of default-configured executors via
 * <li>{@link #sequential(String)}
 * <li>{@link #pooled(String, int)}
 *
 * Supports custom configuration of executors via
 * <li>{@link #configureSequential(String)}
 * <li>{@link #configurePooled(String, int)}
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface ExecutorBuilderFactory<E extends ExecutorPlus, S extends SequentialExecutorPlus>
{
    /**
     * Configure a sequential (single threaded) executor
     */
    ExecutorBuilder<? extends S> configureSequential(String name);

    /**
     * Configure a pooled executor with the requested number of threads
     */
    ExecutorBuilder<? extends E> configurePooled(String name, int threads);

    /**
     * Return a default configuration of sequential executor
     */
    default S sequential(String name) { return configureSequential(name).build(); }

    /**
     * Return a default configuration of pooled executor
     */
    default E pooled(String name, int threads) { return configurePooled(name, threads).build(); }

    /**
     * Entry point for configuring and creating new executors.
     *
     * Supports quick and easy construction of default-configured executors via
     * <li>{@link #sequential(String)}
     * <li>{@link #pooled(String, int)}
     *
     * Supports custom configuration of executors via
     * <li>{@link #configureSequential(String)}
     * <li>{@link #configurePooled(String, int)}
     *
     * Supports any of the above with added JMX registration via sub-factories
     * <li>{@link #withJmx(String)}
     * <li>{@link #withJmxInternal()}
     */
    interface Jmxable<E extends ExecutorPlus, S extends SequentialExecutorPlus> extends ExecutorBuilderFactory<E, S>
    {
        /**
         * @return a factory that configures executors that register against JMX using the provided jmx path
         */
        ExecutorBuilderFactory<E, S> withJmx(String jmxPath);

        /**
         * @return a factory that configures executors that register against JMX using the "internal" jmx path
         */
        default ExecutorBuilderFactory<E, S> withJmxInternal() { return withJmx("internal"); }
    }
}
