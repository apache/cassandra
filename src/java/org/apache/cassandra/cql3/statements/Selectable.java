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

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.cql3.ColumnIdentifier;

public interface Selectable
{

    public static interface Raw
    {
        public Selectable prepare(CFMetaData cfm);

        /**
         * Returns true if any processing is performed on the selected column.
         **/
        public boolean processesSelection();
    }

    public static class WritetimeOrTTL implements Selectable
    {
        public final ColumnIdentifier id;
        public final boolean isWritetime;

        public WritetimeOrTTL(ColumnIdentifier id, boolean isWritetime)
        {
            this.id = id;
            this.isWritetime = isWritetime;
        }

        @Override
        public String toString()
        {
            return (isWritetime ? "writetime" : "ttl") + "(" + id + ")";
        }

        public static class Raw implements Selectable.Raw
        {
            private final ColumnIdentifier.Raw id;
            private final boolean isWritetime;

            public Raw(ColumnIdentifier.Raw id, boolean isWritetime)
            {
                this.id = id;
                this.isWritetime = isWritetime;
            }

            public WritetimeOrTTL prepare(CFMetaData cfm)
            {
                return new WritetimeOrTTL(id.prepare(cfm), isWritetime);
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }

    public static class WithFunction implements Selectable
    {
        public final String functionName;
        public final List<Selectable> args;

        public WithFunction(String functionName, List<Selectable> args)
        {
            this.functionName = functionName;
            this.args = args;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            sb.append(functionName).append("(");
            for (int i = 0; i < args.size(); i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(args.get(i));
            }
            return sb.append(")").toString();
        }

        public static class Raw implements Selectable.Raw
        {
            private final String functionName;
            private final List<Selectable.Raw> args;

            public Raw(String functionName, List<Selectable.Raw> args)
            {
                this.functionName = functionName;
                this.args = args;
            }

            public WithFunction prepare(CFMetaData cfm)
            {
                List<Selectable> preparedArgs = new ArrayList<>(args.size());
                for (Selectable.Raw arg : args)
                    preparedArgs.add(arg.prepare(cfm));
                return new WithFunction(functionName, preparedArgs);
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }

    public static class WithFieldSelection implements Selectable
    {
        public final Selectable selected;
        public final ColumnIdentifier field;

        public WithFieldSelection(Selectable selected, ColumnIdentifier field)
        {
            this.selected = selected;
            this.field = field;
        }

        @Override
        public String toString()
        {
            return String.format("%s.%s", selected, field);
        }

        public static class Raw implements Selectable.Raw
        {
            private final Selectable.Raw selected;
            private final ColumnIdentifier.Raw field;

            public Raw(Selectable.Raw selected, ColumnIdentifier.Raw field)
            {
                this.selected = selected;
                this.field = field;
            }

            public WithFieldSelection prepare(CFMetaData cfm)
            {
                return new WithFieldSelection(selected.prepare(cfm), field.prepare(cfm));
            }

            public boolean processesSelection()
            {
                return true;
            }
        }
    }
}
