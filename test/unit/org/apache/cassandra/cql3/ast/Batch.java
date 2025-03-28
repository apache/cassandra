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

package org.apache.cassandra.cql3.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.cassandra.db.marshal.LongType;

public class Batch implements Statement
{
    public enum Kind { UNLOGGED, COUNTER }
    /*
BEGIN [ UNLOGGED | COUNTER ] BATCH
[USING TTL seconds | TIMESTAMP epoch_in_microseconds]
   dml_statement [USING TIMESTAMP [epoch_microseconds]];
   [dml_statement; ...]
APPLY BATCH;
     */
    public final Optional<Kind> kind; // LOGGED when empty, but if the mutations are for the same partition then promoted to UNLOGGED
    public final Optional<Mutation.Using> using;
    public final List<Mutation> mutations;

    public Batch(Optional<Kind> kind, Optional<Mutation.Using> using, List<Mutation> mutations)
    {
        this.kind = kind;
        this.using = using;
        this.mutations = mutations;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public long timestampOrDefault(long defaultValue)
    {
        if (using.isEmpty()) return defaultValue;
        var opt = using.get().timestamp;
        if (opt.isEmpty()) return defaultValue;
        var timestamp = opt.get();
        return timestamp.get();
    }

    @Override
    public Statement.Kind kind()
    {
        return Statement.Kind.BATCH;
    }

    public boolean isCas()
    {
        return mutations.stream().anyMatch(Mutation::isCas);
    }

    public Batch withoutTimestamp()
    {
        if (using.isEmpty() || using.get().timestamp.isEmpty()) return this;
        return new Batch(kind, using.map(u -> u.withoutTimestamp()), mutations);
    }

    public Batch withTimestamp(long timestamp)
    {
        return withTimestamp(new Mutation.Timestamp(new Literal(timestamp, LongType.instance)));
    }

    public Batch withTimestamp(Mutation.Timestamp timestamp)
    {
        Optional<Mutation.Using> using = this.using.isEmpty()
                                          ? Optional.of(new Mutation.Using(Optional.empty(), Optional.of(timestamp)))
                                          : this.using.map(u -> u.withTimestamp(timestamp));
        return new Batch(kind, using, mutations);
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append("BEGIN");
        kind.ifPresent(k -> sb.append(' ' + k.name()));
        sb.append(" BATCH");
        if (using.isPresent())
        {
            formatter.section(sb);
            using.get().toCQL(sb, formatter);
        }
        formatter.group(sb);
        for (var m : mutations)
        {
            formatter.section(sb);
            m.toCQL(sb, formatter);
            sb.append(';');
        }
        formatter.endgroup(sb);
        formatter.section(sb);
        sb.append("APPLY BATCH");
    }

    @Override
    public Stream<? extends Element> stream()
    {
        List<Element> elements = new ArrayList<>(mutations.size() + (using.isPresent() ? 1 : 0));
        if (using.isPresent())
            elements.add(using.get());
        elements.addAll(mutations);
        return elements.stream();
    }

    @Override
    public Statement visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        boolean updated = false;
        List<Mutation> mutationUpdates = new ArrayList<>(mutations.size());
        for (var m : mutations)
        {
            var m2 = m.visit(v);
            if (!(m2 instanceof Mutation))
                throw new IllegalArgumentException("Batch only supports mutations; given " + m2.getClass() + ", " + m2.detailedToString());
            updated |= m != m2;
            mutationUpdates.add((Mutation) m2);
        }

        return !updated ? this : new Batch(kind, using, mutationUpdates);
    }

    @Override
    public String toString()
    {
        return detailedToString();
    }

    public static class Builder
    {
        public Optional<Kind> kind = Optional.empty();
        public Optional<Mutation.Using> using = Optional.empty();
        public final List<Mutation> mutations = new ArrayList<>();

        public Builder unlogged()
        {
            kind = Optional.of(Kind.UNLOGGED);
            return this;
        }

        public Builder add(Mutation mutation)
        {
            mutations.add(mutation);
            return this;
        }

        public Batch build()
        {
            return new Batch(kind, using, new ArrayList<>(mutations));
        }
    }
}
