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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UserType;

public class Txn implements Statement
{
    // lets
    public final List<Let> lets;
    // return
    public final Optional<Select> output;
    public final Optional<If> ifBlock;
    public final List<Mutation> mutations;

    public Txn(List<Let> lets, Optional<Select> output, Optional<If> ifBlock, List<Mutation> mutations)
    {
        this.lets = lets;
        this.output = output;
        this.ifBlock = ifBlock;
        if (ifBlock.isPresent() && !mutations.isEmpty())
            throw new IllegalArgumentException("Unable to define both IF and non-conditional mutations");
        this.mutations = mutations;
    }

    public static Txn wrap(Select select)
    {
        return new Txn(Collections.emptyList(), Optional.of(select), Optional.empty(), Collections.emptyList());
    }

    public static Txn wrap(Mutation mutation)
    {
        return new Txn(Collections.emptyList(), Optional.empty(), Optional.empty(), Collections.singletonList(mutation));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public void toCQL(StringBuilder sb, CQLFormatter formatter)
    {
        sb.append("BEGIN TRANSACTION");

        formatter.group(sb);
        stream().forEach(e -> {
            formatter.section(sb);
            e.toCQL(sb, formatter);
            if (!(e instanceof If))
                sb.append(';');
        });
        formatter.endgroup(sb);
        formatter.section(sb);
        sb.append("COMMIT TRANSACTION");
    }

    @Override
    public Stream<? extends Element> stream()
    {
        Stream<? extends Element> ret = lets.stream();
        if (output.isPresent())
            ret = Stream.concat(ret, Stream.of(output.get()));
        if (ifBlock.isPresent())
            ret = Stream.concat(ret, Stream.of(ifBlock.get()));
        ret = Stream.concat(ret, mutations.stream());
        return ret;
    }

    @Override
    public String toString()
    {
        return toCQL();
    }

    @Override
    public Kind kind()
    {
        return Kind.TXN;
    }

    @Override
    public Statement visit(Visitor v)
    {
        var u = v.visit(this);
        if (u != this) return u;
        boolean updated = false;
        List<Let> lets = new ArrayList<>(this.lets.size());
        for (Let l : this.lets)
        {
            Statement update = l.select.visit(v);
            if (!(update instanceof Select))
                throw new IllegalArgumentException("Unable to use type " + update.getClass() + " in Let");
            boolean localUpdated = update != l.select;
            updated |= localUpdated;
            lets.add(!localUpdated ? l : new Let(l.symbol, (Select) update));
        }
        Optional<Select> output;
        if (this.output.isPresent())
        {
            Select select = this.output.get();
            Statement update = select.visit(v);
            if (!(update instanceof Select))
                throw new IllegalArgumentException("Unable to use type " + update.getClass() + " in output");
            boolean localUpdated = select != update;
            updated |= localUpdated;
            output = !localUpdated ? this.output : Optional.ofNullable((Select) update);
        }
        else
        {
            output = this.output;
        }
        Optional<If> ifBlock;
        if (this.ifBlock.isPresent())
        {
            If block = this.ifBlock.get();
            boolean localUpdated = false;
            Conditional c = block.conditional.visit(v);
            localUpdated |= c != block.conditional;
            List<Mutation> mutations = new ArrayList<>(this.mutations.size());
            for (Mutation m : block.mutations)
            {
                Statement update = m.visit(v);
                if (!(update instanceof Mutation))
                    throw new IllegalArgumentException("Unable to use type " + update.getClass() + " where Mutation is expected");
                localUpdated |= update != m;
                mutations.add((Mutation) update);
            }
            ifBlock = !localUpdated ? this.ifBlock : Optional.of(new If(c, mutations));
        }
        else
        {
            ifBlock = this.ifBlock;
        }
        List<Mutation> mutations = new ArrayList<>(this.mutations.size());
        for (Mutation m : this.mutations)
        {
            Statement update = m.visit(v);
            if (!(update instanceof Mutation))
                throw new IllegalArgumentException("Unable to use type " + update.getClass() + " where Mutation is expected");
            updated |= update != m;
            mutations.add((Mutation) update);
        }
        if (!updated) return this;
        return new Txn(lets, output, ifBlock, mutations);
    }

    private static void recursiveReferences(Collection<Reference> accum, Reference ref)
    {
        //NOTE: this doesn't actually collect the references due to lack of support in CQL within a Txn, need to fix those cases
        //Coverage: Map/Set/List are a bit annoying as they use real values and not something known in the schema, so may need to handle that different in validation
        AbstractType<?> type = ref.type().unwrap();
        if (type.isCollection())
        {
            //TODO Caleb to add support for [] like normal read/write supports
//            if (type instanceof SetType)
//            {
//                // [value] syntax
//                SetType set = (SetType) type;
//                AbstractType subType = set.getElementsType();

//            }
//            else if (type instanceof MapType)
//            {
//                // [key] syntax
//                MapType map = (MapType) type;
//                AbstractType keyType = map.getKeysType();
//                AbstractType valueType = map.getValuesType();
//            }
            // see Selectable.specForElementOrSlice; ListType is not supported
        }
        else if (type.isUDT())
        {
            //TODO Caleb to support multiple nesting
//            UserType udt = (UserType) type;
//            for (int i = 0; i < udt.size(); i++)
//            {
//                Reference subRef = ref.add(udt.fieldName(i).toString(), udt.type(i));
//                accum.add(subRef);
//                recursiveReferences(accum, subRef);
//            }
        }
    }

    public static class Builder
    {
        private final Map<String, Select> lets = new LinkedHashMap<>();
        // no type system so don't need easy lookup to Expression; just existence check
        private final Set<Reference> allowedReferences = new HashSet<>();
        private Optional<Select> output = Optional.empty();
        private Optional<If> ifBlock = Optional.empty();
        private final List<Mutation> mutations = new ArrayList<>();

        public Set<Reference> allowedReferences()
        {
            return Collections.unmodifiableSet(allowedReferences);
        }

        public Map<String, Select> lets()
        {
            return Collections.unmodifiableMap(lets);
        }

        public boolean isEmpty()
        {
            // don't include output as 'BEGIN TRANSACTION SELECT "000000000000000010000"; COMMIT TRANSACTION' isn't valid
//            return lets.isEmpty();
            // TransactionStatement defines empty as no SELECT or updates
            return !output.isPresent() && !ifBlock.isPresent() && mutations.isEmpty();
        }

        public Builder addLet(String name, Select.Builder select)
        {
            return addLet(name, select.build());
        }

        public Builder addLet(String name, Select select)
        {
            if (lets.containsKey(name))
                throw new IllegalArgumentException("Let name " + name + " already exists");
            lets.put(name, select);

            Reference ref = Reference.of(new Symbol.UnquotedSymbol(name, toNamedTuple(select)));
            for (Expression e : select.selections)
                addAllowedReference(ref.add(e));
            return this;
        }

        private AbstractType<?> toNamedTuple(Select select)
        {
            //TODO don't rely on UserType...
            List<FieldIdentifier> fieldNames = new ArrayList<>(select.selections.size());
            List<AbstractType<?>> fieldTypes = new ArrayList<>(select.selections.size());
            for (Expression e : select.selections)
            {
                fieldNames.add(FieldIdentifier.forQuoted(e.name()));
                fieldTypes.add(e.type());
            }
            return new UserType(null, null, fieldNames, fieldTypes, false);
        }

        private Builder addAllowedReference(Reference ref)
        {
            allowedReferences.add(ref);
            recursiveReferences(allowedReferences, ref);
            return this;
        }

        public Builder addReturn(Select select)
        {
            output = Optional.of(select);
            return this;
        }

        public Builder addReturnReferences(String... names)
        {
            Select.Builder builder = new Select.Builder();
            for (String name : names)
                builder.selection(ref(name));
            addReturn(builder.build());
            return this;
        }

        private Reference ref(String name)
        {
            if (!name.contains("."))
                return Reference.of(new Symbol(name, BytesType.instance));
            String[] splits = name.split("\\.");
            Reference.Builder builder = new Reference.Builder();
            builder.add(splits);
            return builder.build();
        }

        public Builder addIf(If block)
        {
            ifBlock = Optional.of(block);
            return this;
        }

        public Builder addIf(Conditional conditional, Mutation... mutations)
        {
            return addIf(conditional, Arrays.asList(mutations));
        }

        public Builder addIf(Conditional conditional, List<Mutation> mutations)
        {
            return addIf(new If(conditional, mutations));
        }

        public Builder addUpdate(Mutation mutation)
        {
            this.mutations.add(Objects.requireNonNull(mutation));
            return this;
        }

        public Txn build()
        {
            List<Let> lets = this.lets.entrySet().stream().map(e -> new Let(e.getKey(), e.getValue())).collect(Collectors.toList());
            return new Txn(lets, output, ifBlock, new ArrayList<>(mutations));
        }
    }

    public static class If implements Element
    {
        public final Conditional conditional;
        public final List<Mutation> mutations;

        public If(Conditional conditional, List<Mutation> mutations)
        {
            this.conditional = conditional;
            this.mutations = mutations;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("IF ");
            conditional.toCQL(sb, formatter);
            sb.append(" THEN");
            formatter.group(sb);
            for (Mutation mutation : mutations)
            {
                formatter.element(sb);
                mutation.toCQL(sb, formatter);
                sb.append(';');
            }
            formatter.endgroup(sb);
            formatter.section(sb);
            sb.append("END IF");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.concat(Stream.of(conditional), mutations.stream());
        }
    }

    public static class Let implements Element
    {
        public final String symbol;
        public final Select select;

        public Let(String symbol, Select select)
        {
            this.symbol = symbol;
            this.select = select;
        }

        @Override
        public void toCQL(StringBuilder sb, CQLFormatter formatter)
        {
            sb.append("LET ").append(symbol).append(" = (");
            formatter.subgroup(sb);
            select.toCQL(sb, formatter);
            formatter.endsubgroup(sb);
            sb.append(")");
        }

        @Override
        public Stream<? extends Element> stream()
        {
            return Stream.of(select);
        }
    }
}
