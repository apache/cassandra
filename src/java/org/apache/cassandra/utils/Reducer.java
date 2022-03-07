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
package org.apache.cassandra.utils;

/** Accumulator that collects values of type A, and outputs a value of type B. */
public abstract class Reducer<In,Out>
{
    /**
     * @return true if Out is the same as In for the case of a single source iterator
     */
    public boolean singleSourceReduceIsTrivial()
    {
        return false;
    }

    /**
     * combine this object with the previous ones.
     * intermediate state is up to your implementation.
     */
    public abstract void reduce(int idx, In current);

    Throwable errors = null;

    public void error(Throwable error)
    {
        errors = Throwables.merge(errors, error);
    }

    public Throwable getErrors()
    {
        Throwable toReturn = errors;
        errors = null;
        return toReturn;
    }

    /** @return The last object computed by reduce */
    public abstract Out getReduced();

    /**
     * Called at the beginning of each new key, before any reduce is called.
     * To be overridden by implementing classes.
     *
     * Note: There's no need to clear error; merging completes once one is found.
     */
    public void onKeyChange() {}

    public static <In> Reducer<In, In> getIdentity()
    {
        return new IdentityReducer<>();
    }

    private static class IdentityReducer<In> extends Reducer<In, In>
    {
        private In reduced;

        @Override
        public void reduce(int idx, In current)
        {
            this.reduced = current;
        }

        @Override
        public In getReduced()
        {
            return reduced;
        }

        @Override
        public void onKeyChange() {
            this.reduced = null;
        }

        @Override
        public boolean singleSourceReduceIsTrivial()
        {
            return true;
        }
    }
}
