/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.concurrent;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * An abstraction for Transactional behaviour. An object implementing this interface has a lifetime
 * of the following pattern:
 *
 * Throwable failure = null;
 * try (Transactional t1, t2 = ...)
 * {
 *     // do work with t1 and t2
 *     t1.prepareToCommit();
 *     t2.prepareToCommit();
 *     failure = t1.commit(failure);
 *     failure = t2.commit(failure);
 * }
 * logger.error(failure);
 *
 * If something goes wrong before commit() is called on any transaction, then on exiting the try block
 * the auto close method should invoke cleanup() and then abort() to reset any state.
 * If everything completes normally, then on exiting the try block the auto close method will invoke cleanup
 * to release any temporary state/resources
 *
 * No exceptions should be thrown during commit; if they are, it is not at all clear what the correct behaviour
 * of the system should be, and so simply logging the exception is likely best (since it may have been an issue
 * during cleanup, say), and rollback cannot now occur. As such all exceptions and assertions that may be thrown
 * should be checked and ruled out during commit preparation.
 */
public interface Transactional extends AutoCloseable
{

    /**
     * A simple abstract implementation of Transactional behaviour.
     * In general this should be used as the base class for any transactional implementations.
     *
     * If the implementation wraps any internal Transactional objects, it must proxy every
     * commit() and abort() call onto each internal object to ensure correct behaviour
     */
    public static abstract class AbstractTransactional implements Transactional
    {
        public static enum State
        {
            IN_PROGRESS,
            READY_TO_COMMIT,
            COMMITTED,
            ABORTED;
        }

        private State state = State.IN_PROGRESS;

        // the methods for actually performing the necessary behaviours, that are themselves protected against
        // improper use by the external implementations provided by this class. empty default implementations
        // could be provided, but we consider it safer to force implementers to consider explicitly their presence

        protected abstract Throwable doCommit(Throwable accumulate);
        protected abstract Throwable doAbort(Throwable accumulate);

        // this only needs to perform cleanup of state unique to this instance; any internal
        // Transactional objects will perform cleanup in the commit() or abort() calls
        protected abstract Throwable doCleanup(Throwable accumulate);

        /**
         * Do any preparatory work prior to commit. This method should throw any exceptions that can be encountered
         * during the finalization of the behaviour.
         */
        protected abstract void doPrepare();

        /**
         * commit any effects of this transaction object graph, then cleanup; delegates first to doCommit, then to doCleanup
         */
        public final Throwable commit(Throwable accumulate)
        {
            if (state != State.READY_TO_COMMIT)
                throw new IllegalStateException("Commit attempted before prepared to commit");
            accumulate = doCommit(accumulate);
            accumulate = doCleanup(accumulate);
            state = State.COMMITTED;
            return accumulate;
        }

        /**
         * rollback any effects of this transaction object graph; delegates first to doCleanup, then to doAbort
         */
        public final Throwable abort(Throwable accumulate)
        {
            if (state == State.ABORTED)
                return accumulate;
            if (state == State.COMMITTED)
            {
                try
                {
                    throw new IllegalStateException("Attempted to abort a committed operation");
                }
                catch (Throwable t)
                {
                    accumulate = merge(accumulate, t);
                }
                return accumulate;
            }
            state = State.ABORTED;
            // we cleanup first so that, e.g., file handles can be released prior to deletion
            accumulate = doCleanup(accumulate);
            accumulate = doAbort(accumulate);
            return accumulate;
        }

        // if we are committed or aborted, then we are done; otherwise abort
        public final void close()
        {
            switch (state)
            {
                case COMMITTED:
                case ABORTED:
                    break;
                default:
                    abort();
            }
        }

        /**
         * The first phase of commit: delegates to doPrepare(), with valid state transition enforcement.
         * This call should be propagated onto any child objects participating in the transaction
         */
        public final void prepareToCommit()
        {
            if (state != State.IN_PROGRESS)
                throw new IllegalStateException("Cannot prepare to commit unless IN_PROGRESS; state is " + state);

            doPrepare();
            state = State.READY_TO_COMMIT;
        }

        /**
         * convenience method to both prepareToCommit() and commit() in one operation;
         * only of use to outer-most transactional object of an object graph
         */
        public Object finish()
        {
            prepareToCommit();
            commit();
            return this;
        }

        // convenience method wrapping abort, and throwing any exception encountered
        // only of use to (and to be used by) outer-most object in a transactional graph
        public final void abort()
        {
            maybeFail(abort(null));
        }

        // convenience method wrapping commit, and throwing any exception encountered
        // only of use to (and to be used by) outer-most object in a transactional graph
        public final void commit()
        {
            maybeFail(commit(null));
        }

        public final State state()
        {
            return state;
        }
    }

    // commit should generally never throw an exception, and preferably never generate one,
    // but if it does generate one it should accumulate it in the parameter and return the result
    // IF a commit implementation has a real correctness affecting exception that cannot be moved to
    // prepareToCommit, it MUST be executed before any other commit methods in the object graph
    public Throwable commit(Throwable accumulate);

    // release any resources, then rollback all state changes (unless commit() has already been invoked)
    public Throwable abort(Throwable accumulate);

    public void prepareToCommit();
}
