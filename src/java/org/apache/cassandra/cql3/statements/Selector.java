/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.cql3.statements;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.Term;

public abstract class Selector
{
    public enum Function
    {
        WRITE_TIME, TTL;

        @Override
        public String toString()
        {
            switch (this)
            {
                case WRITE_TIME:
                    return "writetime";
                case TTL:
                    return "ttl";
            }
            throw new AssertionError();
        }
    }

    public abstract ColumnIdentifier id();

    public boolean hasFunction()
    {
        return false;
    }

    public Function function()
    {
        return null;
    }

    public boolean hasKey()
    {
        return false;
    }

    public Term key()
    {
        return null;
    }

    public static class WithFunction extends Selector
    {
        private final Function function;
        private final ColumnIdentifier id;

        public WithFunction(ColumnIdentifier id, Function function)
        {
            this.id = id;
            this.function = function;
        }

        public ColumnIdentifier id()
        {
            return id;
        }

        @Override
        public boolean hasFunction()
        {
            return true;
        }

        @Override
        public Function function()
        {
            return function;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(function, id);
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof WithFunction))
                return false;
            Selector that = (WithFunction)o;
            return id().equals(that.id()) && function() == that.function();
        }

        @Override
        public String toString()
        {
            return function + "(" + id + ")";
        }
    }

    public static class WithKey extends Selector
    {
        private final ColumnIdentifier id;
        private final Term key;

        public WithKey(ColumnIdentifier id, Term key)
        {
            this.id = id;
            this.key = key;
        }

        public ColumnIdentifier id()
        {
            return id;
        }

        @Override
        public boolean hasKey()
        {
            return true;
        }

        public Term key()
        {
            return key;
        }

        @Override
        public final int hashCode()
        {
            return Objects.hashCode(id, key);
        }

        @Override
        public final boolean equals(Object o)
        {
            if(!(o instanceof WithKey))
                return false;
            WithKey that = (WithKey)o;
            return id().equals(that.id()) && key.equals(that.key);
        }

        @Override
        public String toString()
        {
            return id + "[" + key + "]";
        }
    }
}
