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
package org.apache.cassandra.service.thresholds;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.slf4j.Logger;

import io.netty.util.concurrent.FastThreadLocal;

/**
 * Generic ThreadLocal state manager for coordinator warnings.
 * Provides lifecycle management (init, update, done, reset) for accumulating
 * warnings across a client request.
 *
 * @param <S> the type of state to manage (e.g., Map, custom holder class)
 */
public class CoordinatorWarningsState<S>
{
    private final FastThreadLocal<S> threadLocal;
    private final S initSentinel;
    private final S emptySentinel;
    private final Supplier<S> stateFactory;
    private final Logger logger;
    private final String name;
    private final boolean enableDefensiveChecks;

    /**
     * Creates a new coordinator warnings state manager.
     *
     * @param name descriptive name for logging
     * @param initSentinel sentinel value indicating initialized but empty state
     * @param emptySentinel sentinel value indicating cleared state
     * @param stateFactory factory to create new mutable state instances
     * @param logger logger for diagnostic messages
     * @param enableDefensiveChecks whether to enable defensive checks
     */
    public CoordinatorWarningsState(String name,
                                    S initSentinel,
                                    @Nullable S emptySentinel,
                                    Supplier<S> stateFactory,
                                    Logger logger,
                                    boolean enableDefensiveChecks)
    {
        this.name = name;
        this.initSentinel = initSentinel;
        this.emptySentinel = emptySentinel;
        this.stateFactory = stateFactory;
        this.logger = logger;
        this.enableDefensiveChecks = enableDefensiveChecks;
        this.threadLocal = new FastThreadLocal<>();
    }

    /**
     * Initialize state for this thread.
     * Must be called at the start of a client request.
     */
    public void init()
    {
        if (logger.isTraceEnabled())
            logger.trace("{}.init()", name);

        S current = threadLocal.get();
        if (current != null && enableDefensiveChecks)
        {
            throw new AssertionError(name + ".init called while state is not null: " + current);
        }
        threadLocal.set(initSentinel);
    }

    /**
     * Reset/clear state for this thread.
     */
    public void reset()
    {
        if (logger.isTraceEnabled())
            logger.trace("{}.reset()", name);

        if (emptySentinel != null)
            threadLocal.set(emptySentinel);
        else
            threadLocal.remove();
    }

    /**
     * Get current state, creating mutable state if needed.
     * Transitions from initSentinel to mutable state on first access.
     *
     * @param fallbackSupplier supplies fallback state if not initialized
     * @return current mutable state
     */
    public S getMutableState(Supplier<S> fallbackSupplier)
    {
        S state = threadLocal.get();

        if (state == null)
        {
            if (enableDefensiveChecks)
                throw new AssertionError(name + " accessing state without calling .init() first");
            return fallbackSupplier.get();
        }

        if (state == initSentinel)
        {
            state = stateFactory.get();
            threadLocal.set(state);
        }

        return state;
    }

    /**
     * Check if state is initialized and not empty.
     */
    public boolean isActive()
    {
        S state = threadLocal.get();
        return state != null && state != initSentinel && state != emptySentinel;
    }

    /**
     * Process accumulated state and reset.
     * Typical pattern for done() implementations.
     *
     * @param stateChecker checks if state should be processed (e.g., isEmpty())
     * @param processor processes the state
     * @param errorHandler handles any errors during processing
     */
    public void processAndReset(StateChecker<S> stateChecker,
                                Consumer<S> processor,
                                BiConsumer<S, Exception> errorHandler)
    {
        try
        {
            S state = threadLocal.get();
            if (state == null || state == initSentinel || !stateChecker.shouldProcess(state))
                return;

            processor.accept(state);
        }
        catch (Exception e)
        {
            S state = threadLocal.get();
            if (errorHandler != null)
                errorHandler.accept(state, e);
            else
                logger.error("Error processing " + name, e);
        }
        finally
        {
            reset();
        }
    }

    /**
     * Functional interface for checking if state should be processed.
     */
    @FunctionalInterface
    public interface StateChecker<S>
    {
        boolean shouldProcess(S state);
    }
}