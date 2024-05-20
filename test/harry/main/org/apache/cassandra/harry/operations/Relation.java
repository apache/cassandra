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

package org.apache.cassandra.harry.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.DataGenerators;

public class Relation
{
    public final RelationKind kind;
    public final ColumnSpec<?> columnSpec;
    // Theoretically, in model, we'll just be able to compare stuff according to relation, and pass it to DB
    public long descriptor;

    Relation(RelationKind kind,
             ColumnSpec<?> columnSpec,
             long descriptor)
    {
        this.kind = kind;
        this.columnSpec = columnSpec;
        this.descriptor = descriptor;
    }

    public boolean match(long l)
    {
        // TODO: there are == NULL queries
        if (l == DataGenerators.NIL_DESCR || l == DataGenerators.UNSET_DESCR)
            return false;

        return kind.match(columnSpec.type.generator()::compare, l, descriptor);
    }

    public Object value()
    {
        return columnSpec.inflate(descriptor);
    }

    public String column()
    {
        return columnSpec.name;
    }

    public String toClause()
    {
        return kind.getClause(column());
    }

    public String toString()
    {
        return "Relation{" +
               "kind=" + kind +
               ", columnSpec=" + columnSpec +
               ", descriptor=" + descriptor + " (" + Long.toHexString(descriptor) + ")" +
               '}';
    }

    public static Relation relation(RelationKind kind, ColumnSpec<?> columnSpec, long descriptor)
    {
        return new Relation(kind, columnSpec, descriptor);
    }

    public static Relation eqRelation(ColumnSpec<?> columnSpec, long descriptor)
    {
        return new Relation(RelationKind.EQ, columnSpec, descriptor);
    }

    public static List<Relation> eqRelations(long[] key, List<ColumnSpec<?>> columnSpecs)
    {
        List<Relation> relations = new ArrayList<>(key.length);
        addEqRelation(key, columnSpecs, relations);
        return relations;
    }

    public static void addEqRelation(long[] key, List<ColumnSpec<?>> columnSpecs, List<Relation> relations)
    {
        addRelation(key, columnSpecs, relations, RelationKind.EQ);
    }

    public static void addRelation(long[] key, List<ColumnSpec<?>> columnSpecs, List<Relation> relations, RelationKind kind)
    {
        assert key.length == columnSpecs.size() || key.length > DataGenerators.KeyGenerator.MAX_UNIQUE_PREFIX_COLUMNS :
        String.format("Key size (%d) should equal to column spec size (%d). Specs: %s", key.length, columnSpecs.size(), columnSpecs);
        for (int i = 0; i < key.length; i++)
        {
            ColumnSpec<?> spec = columnSpecs.get(i);
            relations.add(relation(kind, spec, key[i]));
        }
    }

    public enum RelationKind
    {
        LT
        {
            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return false;
            }

            public RelationKind negate()
            {
                return GT;
            }

            public long nextMatch(long n)
            {
                return Math.subtractExact(n, 1);
            }

            public String toString()
            {
                return "<";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) < 0;
            }
        },
        GT
        {
            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return false;
            }

            public RelationKind negate()
            {
                return LT;
            }

            public String toString()
            {
                return ">";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) > 0;
            }

            public long nextMatch(long n)
            {
                return Math.addExact(n, 1);
            }
        },
        LTE
        {
            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                return GTE;
            }

            public String toString()
            {
                return "<=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) <= 0;
            }

            public long nextMatch(long n)
            {
                return Math.subtractExact(n, 1);
            }
        },
        GTE
        {
            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                return LTE;
            }

            public String toString()
            {
                return ">=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) >= 0;
            }

            public long nextMatch(long n)
            {
                return Math.addExact(n, 1);
            }
        },
        EQ
        {
            public boolean isNegatable()
            {
                return false;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                throw new IllegalArgumentException("Cannot negate EQ");
            }

            public long nextMatch(long n)
            {
                return n;
            }

            public String toString()
            {
                return "=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) == 0;
            }
        };

        public abstract boolean match(LongComparator comparator, long l, long r);

        public String getClause(String name)
        {
            return String.format("%s %s ?", name, toString());
        }

        public String getClause(ColumnSpec<?> spec)
        {
            return getClause(spec.name);
        }

        public abstract boolean isNegatable();

        public abstract boolean isInclusive();

        public abstract RelationKind negate();

        public abstract long nextMatch(long n);
    }

    public static interface LongComparator
    {
        public int compare(long l, long r);
    }

    public static LongComparator FORWARD_COMPARATOR = Long::compare;
}
