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

package org.apache.cassandra.harry.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.Element;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.ExpressionEvaluator;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ImmutableUniqueList;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.harry.model.BytesPartitionState.asCQL;

public class ASTSingleTableModel
{
    public final BytesPartitionState.Factory factory;
    private final TreeMap<BytesPartitionState.Ref, BytesPartitionState> partitions = new TreeMap<>();

    public ASTSingleTableModel(TableMetadata metadata)
    {
        this.factory = new BytesPartitionState.Factory(metadata);
    }

    public NavigableSet<BytesPartitionState.Ref> partitionKeys()
    {
        return partitions.navigableKeySet();
    }

    public int size()
    {
        return partitions.size();
    }

    public boolean isEmpty()
    {
        return partitions.isEmpty();
    }

    public TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> index(Symbol symbol)
    {
        if (factory.partitionColumns.contains(symbol))
            return indexPartitionColumn(symbol);
        if (factory.staticColumns.contains(symbol))
            return indexStaticColumn(symbol);
        return indexRowColumn(symbol);
    }

    private TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> indexPartitionColumn(Symbol symbol)
    {
        int offset = factory.partitionColumns.indexOf(symbol);
        TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> index = new TreeMap<>(symbol.type()::compare);
        for (BytesPartitionState partition : partitions.values())
        {
            if (partition.isEmpty()) continue;
            ByteBuffer bb = partition.key.bufferAt(offset);
            List<BytesPartitionState.PrimaryKey> list = index.computeIfAbsent(bb, i -> new ArrayList<>());
            for (BytesPartitionState.Row row : partition.rows())
                list.add(row.ref());
        }
        return index;
    }

    private TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> indexStaticColumn(Symbol symbol)
    {
        TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> index = new TreeMap<>(symbol.type()::compare);
        for (BytesPartitionState partition : partitions.values())
        {
            if (partition.isEmpty()) continue;
            ByteBuffer bb = partition.staticRow().get(symbol);
            if (bb == null)
                continue;
            List<BytesPartitionState.PrimaryKey> list = index.computeIfAbsent(bb, i -> new ArrayList<>());
            for (BytesPartitionState.Row row : partition.rows())
                list.add(row.ref());
        }
        return index;
    }

    private TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> indexRowColumn(Symbol symbol)
    {
        boolean clustering = factory.clusteringColumns.contains(symbol);
        int offset = clustering ? factory.clusteringColumns.indexOf(symbol) : factory.regularColumns.indexOf(symbol);
        TreeMap<ByteBuffer, List<BytesPartitionState.PrimaryKey>> index = new TreeMap<>(symbol.type()::compare);
        for (BytesPartitionState partition : partitions.values())
        {
            if (partition.isEmpty()) continue;
            for (BytesPartitionState.Row row : partition.rows())
            {
                ByteBuffer bb = clustering ? row.clustering.bufferAt(offset) : row.get(offset);
                if (bb == null)
                    continue;
                index.computeIfAbsent(bb, i -> new ArrayList<>()).add(row.ref());
            }
        }
        return index;
    }

    public void update(Mutation mutation)
    {
        switch (mutation.kind)
        {
            case INSERT:
                update((Mutation.Insert) mutation);
                break;
            case UPDATE:
                update((Mutation.Update) mutation);
                break;
            case DELETE:
                update((Mutation.Delete) mutation);
                break;
            default:
                throw new UnsupportedOperationException(mutation.kind.name());
        }
    }

    public void update(Mutation.Insert insert)
    {
        Clustering<ByteBuffer> pd = pd(insert);
        BytesPartitionState partition = partitions.get(factory.createRef(pd));
        if (partition == null)
        {
            partition = factory.create(pd);
            partitions.put(partition.ref(), partition);
        }
        Map<Symbol, Expression> values = insert.values;
        if (!factory.staticColumns.isEmpty() && !Sets.intersection(factory.staticColumns.asSet(), values.keySet()).isEmpty())
        {
            // static columns to add in.  If we are doing something like += to a row that doesn't exist, we still update statics...
            Map<Symbol, ByteBuffer> write = new HashMap<>();
            for (Symbol col : Sets.intersection(factory.staticColumns.asSet(), values.keySet()))
                write.put(col, eval(values.get(col)));
            partition.setStaticColumns(write);
        }
        Map<Symbol, ByteBuffer> write = new HashMap<>();
        for (Symbol col : Sets.intersection(factory.regularColumns.asSet(), values.keySet()))
            write.put(col, eval(values.get(col)));
        partition.setColumns(key(insert.values, factory.clusteringColumns),
                             write,
                             true);
    }

    public void update(Mutation.Update update)
    {
        var split = splitOnPartition(update.where.simplify());
        List<Clustering<ByteBuffer>> pks = split.left;
        List<Conditional> remaining = split.right;
        for (Clustering<ByteBuffer> pd : pks)
        {
            BytesPartitionState partition = partitions.get(factory.createRef(pd));
            if (partition == null)
            {
                partition = factory.create(pd);
                partitions.put(partition.ref(), partition);
            }
            Map<Symbol, Expression> set = update.set;
            if (!factory.staticColumns.isEmpty() && !Sets.intersection(factory.staticColumns.asSet(), set.keySet()).isEmpty())
            {
                // static columns to add in.  If we are doing something like += to a row that doesn't exist, we still update statics...
                Map<Symbol, ByteBuffer> write = new HashMap<>();
                for (Symbol col : Sets.intersection(factory.staticColumns.asSet(), set.keySet()))
                    write.put(col, eval(set.get(col)));
                partition.setStaticColumns(write);
            }
            for (Clustering<ByteBuffer> cd : clustering(remaining))
            {
                Map<Symbol, ByteBuffer> write = new HashMap<>();
                for (Symbol col : Sets.intersection(factory.regularColumns.asSet(), set.keySet()))
                    write.put(col, eval(set.get(col)));

                partition.setColumns(cd, write, false);
            }
        }
    }

    private enum DeleteKind
    {PARTITION, ROW, COLUMN}

    public void update(Mutation.Delete delete)
    {
        //TODO (coverage): range deletes
        var split = splitOnPartition(delete.where.simplify());
        List<Clustering<ByteBuffer>> pks = split.left;
        List<Clustering<ByteBuffer>> clusterings = split.right.isEmpty() ? Collections.emptyList() : clustering(split.right);
        HashSet<Symbol> columns = delete.columns.isEmpty() ? null : new HashSet<>(delete.columns);
        for (Clustering<ByteBuffer> pd : pks)
        {
            BytesPartitionState partition = partitions.get(factory.createRef(pd));
            if (partition == null) return; // can't delete a partition that doesn't exist...

            DeleteKind kind = DeleteKind.PARTITION;
            if (!delete.columns.isEmpty())
                kind = DeleteKind.COLUMN;
            else if (!clusterings.isEmpty())
                kind = DeleteKind.ROW;

            switch (kind)
            {
                case PARTITION:
                    partitions.remove(partition.ref());
                    break;
                case ROW:
                    for (Clustering<ByteBuffer> cd : clusterings)
                    {
                        partition.deleteRow(cd);
                        if (partition.shouldDelete())
                            partitions.remove(partition.ref());
                    }
                    break;
                case COLUMN:
                    if (clusterings.isEmpty())
                    {
                        partition.deleteStaticColumns(columns);
                    }
                    else
                    {
                        for (Clustering<ByteBuffer> cd : clusterings)
                        {
                            partition.deleteColumns(cd, columns);
                            if (partition.shouldDelete())
                                partitions.remove(partition.ref());
                        }
                    }
                    break;
//                case SLICE:
//                case RANGE:
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    private List<Clustering<ByteBuffer>> clustering(List<Conditional> conditionals)
    {
        if (conditionals.isEmpty())
        {
            if (factory.clusteringColumns.isEmpty()) return Collections.singletonList(Clustering.EMPTY);
            throw new IllegalArgumentException("No clustering columns defined in the WHERE clause, but clustering columns exist; expected " + factory.clusteringColumns);
        }
        var split = splitOnClustering(conditionals);
        var clusterings = split.left;
        var remaining = split.right;
        if (!remaining.isEmpty())
            throw new IllegalArgumentException("Non Partition/Clustering columns found in WHERE clause; " + remaining.stream().map(Element::toCQL).collect(Collectors.joining(", ")));
        return clusterings;
    }

    private Pair<List<Clustering<ByteBuffer>>, List<Conditional>> splitOnPartition(List<Conditional> conditionals)
    {
        return splitOn(factory.partitionColumns.asSet(), conditionals);
    }

    private Pair<List<Clustering<ByteBuffer>>, List<Conditional>> splitOnClustering(List<Conditional> conditionals)
    {
        return splitOn(factory.clusteringColumns.asSet(), conditionals);
    }

    private Pair<List<Clustering<ByteBuffer>>, List<Conditional>> splitOn(ImmutableUniqueList<Symbol>.AsSet columns, List<Conditional> conditionals)
    {
        // pk requires equality
        Map<Symbol, Set<ByteBuffer>> pks = new HashMap<>();
        List<Conditional> other = new ArrayList<>();
        for (Conditional c : conditionals)
        {
            if (c instanceof Conditional.Where)
            {
                Conditional.Where w = (Conditional.Where) c;
                if (w.kind == Conditional.Where.Inequality.EQUAL && columns.contains(w.lhs))
                {
                    Symbol col = (Symbol) w.lhs;
                    ByteBuffer bb = eval(w.rhs);
                    if (pks.containsKey(col))
                        throw new IllegalArgumentException("Partition column " + col + " was defined multiple times in the WHERE clause");
                    pks.put(col, Collections.singleton(bb));
                }
                else
                {
                    other.add(c);
                }
            }
            else if (c instanceof Conditional.In)
            {
                Conditional.In i = (Conditional.In) c;
                if (columns.contains(i.ref))
                {
                    Symbol col = (Symbol) i.ref;
                    if (pks.containsKey(col))
                        throw new IllegalArgumentException("Partition column " + col + " was defined multiple times in the WHERE clause");
                    var set = i.expressions.stream().map(ASTSingleTableModel::eval).collect(Collectors.toSet());
                    pks.put(col, set);
                }
                else
                {
                    other.add(c);
                }
            }
            else
            {
                other.add(c);
            }
        }
        if (!columns.equals(pks.keySet()))
        {
            var missing = Sets.difference(columns, pks.keySet());
            throw new AssertionError("Unable to find expected columns " + missing);
        }

        List<Clustering<ByteBuffer>> partitionKeys = keys(columns, pks);
        return Pair.create(partitionKeys, other);
    }

    private List<Clustering<ByteBuffer>> keys(Collection<Symbol> columns, Map<Symbol, Set<ByteBuffer>> pks)
    {
        //TODO (coverage): handle IN
        ByteBuffer[] bbs = new ByteBuffer[columns.size()];
        int idx = 0;
        for (Symbol s : columns)
        {
            Set<ByteBuffer> values = pks.get(s);
            if (values.size() > 1)
                throw new UnsupportedOperationException("IN clause is currently unsupported... its on the backlog!");
            bbs[idx++] = Iterables.getFirst(values, null);
        }
        return Collections.singletonList(BufferClustering.make(bbs));
    }

    private Clustering<ByteBuffer> pd(Mutation.Insert mutation)
    {
        return key(mutation.values, factory.partitionColumns);
    }

    public BytesPartitionState get(BytesPartitionState.Ref ref)
    {
        return partitions.get(ref);
    }

    public List<BytesPartitionState> getByToken(Token token)
    {
        NavigableSet<BytesPartitionState.Ref> keys = partitions.navigableKeySet();
        // To support the case where 2+ keys share the same token, need to create a token ref before and after the token, to make sure
        // the head/tail sets find the matches correctly
        NavigableSet<BytesPartitionState.Ref> matches = keys.headSet(factory.createRef(token, true), true)
                                                            .tailSet(factory.createRef(token, false), true);
        if (matches.isEmpty()) return Collections.emptyList();
        return matches.stream().map(partitions::get).collect(Collectors.toList());
    }

    public void validate(ByteBuffer[][] actual, Select select)
    {
        SelectResult results = getRowsAsByteBuffer(select);
        if (results.unordered)
        {
            validateAnyOrder(factory.selectionOrder, toRow(factory.selectionOrder, actual), toRow(factory.selectionOrder, results.rows));
        }
        else
        {
            validate(actual, results.rows);
        }
    }

    public void validate(ByteBuffer[][] actual, ByteBuffer[][] expected)
    {
        validate(factory.selectionOrder, actual, expected);
    }

    private static void validate(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] actual, ByteBuffer[][] expected)
    {
        // check any order
        validateAnyOrder(columns, toRow(columns, actual), toRow(columns, expected));
        validateOrder(columns, actual, expected);
    }

    private static void validateAnyOrder(ImmutableUniqueList<Symbol> columns, Set<Row> actual, Set<Row> expected)
    {
        var unexpected = Sets.difference(actual, expected);
        var missing = Sets.difference(expected, actual);
        StringBuilder sb = null;
        if (!unexpected.isEmpty())
        {
            sb = new StringBuilder();
            sb.append("Unexpected rows found:\n").append(table(columns, unexpected));
        }

        if (!missing.isEmpty())
        {
            if (sb == null)
            {
                sb = new StringBuilder();
            }
            else
            {
                sb.append('\n');
            }
            if (actual.isEmpty()) sb.append("No rows returned");
            else sb.append("Missing rows:\n").append(table(columns, missing));
        }
        if (sb != null)
        {
            sb.append("\nExpected:\n").append(table(columns, expected));
            throw new AssertionError(sb.toString());
        }
    }

    private static String table(ImmutableUniqueList<Symbol> columns, Collection<Row> rows)
    {
        return TableBuilder.toStringPiped(columns.stream().map(Symbol::toCQL).collect(Collectors.toList()),
                                          // intellij or junit can be tripped up by utf control or invisible chars, so this logic tries to normalize to make things more safe
                                          () -> rows.stream()
                                         .map(r -> r.asCQL().stream().map(StringUtils::escapeControlChars).collect(Collectors.toList()))
                                         .iterator());
    }

    private static String table(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows)
    {
        return TableBuilder.toStringPiped(columns.stream().map(Symbol::toCQL).collect(Collectors.toList()),
                                          () -> Stream.of(rows).map(row -> asCQL(columns, row)).iterator());
    }

    private static void validateOrder(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] actual, ByteBuffer[][] expected)
    {
        StringBuilder sb = null;
        for (int i = 0, size = Math.min(actual.length, expected.length); i < size; i++)
        {
            ByteBuffer[] as = actual[i];
            ByteBuffer[] es = expected[i];
            if (as.length != es.length)
            {
                if (sb == null)
                    sb = new StringBuilder();
                sb.append("\nExpected number of columns does not match");
            }
            for (int c = 0, cs = Math.min(as.length, es.length); c < cs; c++)
            {
                ByteBuffer a = as[c];
                ByteBuffer e = es[c];
                if (!Objects.equals(a, e))
                {
                    Symbol symbol = columns.get(c);
                    if (sb == null)
                        sb = new StringBuilder();
                    sb.append(String.format("\nIncorrect value for row %d column %s: expected %s but was %s", i, symbol,
                                            e == null ? "null" : symbol.type().asCQL3Type().toCQLLiteral(e),
                                            a == null ? "null" : symbol.type().asCQL3Type().toCQLLiteral(a)));
                }
            }
        }

        if (sb != null)
        {
            sb.append("\nExpected:\n").append(table(columns, expected));
            throw new AssertionError(sb.toString());
        }
    }

    private static Set<Row> toRow(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows)
    {
        Set<Row> set = new HashSet<>();
        for (ByteBuffer[] row : rows)
            set.add(new Row(columns, row));
        return set;
    }

    private static class SelectResult
    {
        private final ByteBuffer[][] rows;
        private final boolean unordered;

        private SelectResult(ByteBuffer[][] rows, boolean unordered)
        {
            this.rows = rows;
            this.unordered = unordered;
        }
    }

    private SelectResult getRowsAsByteBuffer(Select select)
    {
        LookupContext ctx = context(select);
        List<BytesPartitionState.PrimaryKey> primaryKeys;
        if (ctx.unmatchable)
        {
            primaryKeys = Collections.emptyList();
        }
        else if (ctx.eq.keySet().containsAll(factory.partitionColumns))
        {
            primaryKeys = findByPartitionEq(ctx);
        }
        else if (ctx.token != null)
        {
            primaryKeys = findKeysByToken(ctx);
        }
        else if (ctx.tokenLowerBound != null || ctx.tokenUpperBound != null)
        {
            primaryKeys = findKeysByTokenSearch(ctx);
        }
        else
        {
            primaryKeys = search(ctx);
        }
        //TODO (correctness): now that we have the rows we need to handle the selections/aggregation/limit/group-by/etc.
        return new SelectResult(getRowsAsByteBuffer(primaryKeys), ctx.unordered);
    }

    public ByteBuffer[][] getRowsAsByteBuffer(List<BytesPartitionState.PrimaryKey> primaryKeys)
    {
        ByteBuffer[][] rows = new ByteBuffer[primaryKeys.size()][];
        int idx = 0;
        for (BytesPartitionState.PrimaryKey pk : primaryKeys)
        {
            BytesPartitionState partition = partitions.get(pk.partition);
            BytesPartitionState.Row row = partition.get(pk.clustering);
            rows[idx++] = getRowAsByteBuffer(partition, row);
        }
        return rows;
    }

    public ByteBuffer[][] getRowsAsByteBufferFromPartitions(List<BytesPartitionState> ps)
    {
        List<ByteBuffer[]> rows = new ArrayList<>();
        for (BytesPartitionState p : ps)
        {
            for (BytesPartitionState.Row row : p.rows())
                rows.add(getRowAsByteBuffer(p, row));
        }
        return rows.toArray(ByteBuffer[][]::new);
    }

    private ByteBuffer[] getRowAsByteBuffer(BytesPartitionState partition, @Nullable BytesPartitionState.Row row)
    {
        Clustering<ByteBuffer> pd = partition.key;
        BytesPartitionState.Row staticRow = partition.staticRow();
        ByteBuffer[] bbs = new ByteBuffer[factory.selectionOrder.size()];
        for (Symbol col : factory.partitionColumns)
            bbs[factory.selectionOrder.indexOf(col)] = pd.bufferAt(factory.partitionColumns.indexOf(col));
        for (Symbol col : factory.staticColumns)
            bbs[factory.selectionOrder.indexOf(col)] = staticRow.get(col);
        if (row != null)
        {
            for (Symbol col : factory.clusteringColumns)
                bbs[factory.selectionOrder.indexOf(col)] = row.clustering.bufferAt(factory.clusteringColumns.indexOf(col));
            for (Symbol col : factory.regularColumns)
                bbs[factory.selectionOrder.indexOf(col)] = row.get(col);
        }
        return bbs;
    }

    private LookupContext context(Select select)
    {
        if (select.where.isEmpty())
            throw new IllegalArgumentException("Select without a where clause is currently unsupported");
        LookupContext ctx = new LookupContext(select);
        context(ctx, select.where.get());
        maybeNormalizeTokenBounds(ctx);
        return ctx;
    }

    private void maybeNormalizeTokenBounds(LookupContext ctx)
    {
        if (ctx.tokenLowerBound != null && ctx.tokenUpperBound != null)
        {
            int rc = ctx.tokenLowerBound.token.compareTo(ctx.tokenUpperBound.token);
            if (rc > 0)
            {
                // where token > 10 and < 0.... nothing matches that!
                ctx.unmatchable = true;
                ctx.tokenLowerBound = null;
                ctx.tokenUpperBound = null;
            }
            else if (rc == 0)
            {
                // tokens match... but is _EQ allowed for both cases?
                if (!(ctx.tokenLowerBound.inequality == Conditional.Where.Inequality.GREATER_THAN_EQ
                      && ctx.tokenUpperBound.inequality == Conditional.Where.Inequality.LESS_THAN_EQ))
                {
                    // token < 42 and >= 42... nothing matches that!
                    ctx.unmatchable = true;
                    ctx.tokenLowerBound = null;
                    ctx.tokenUpperBound = null;
                }
            }
        }
    }

    private void context(LookupContext ctx, Conditional conditional)
    {
        if (conditional instanceof Conditional.Where)
        {
            Conditional.Where w = (Conditional.Where) conditional;
            if (w.kind == Conditional.Where.Inequality.NOT_EQUAL)
                throw new UnsupportedOperationException("!= is currently not supported");
            if (w.lhs instanceof Symbol)
            {
                Symbol col = (Symbol) w.lhs;
                switch (w.kind)
                {
                    case EQUAL:
                        var override = ctx.eq.put(col, Collections.singletonList(w.rhs));
                        if (override != null)
                            throw new IllegalStateException("Column " + col.detailedName() + " had 2 '=' statements...");
                        break;
                    case LESS_THAN:
                    case LESS_THAN_EQ:
                    case GREATER_THAN:
                    case GREATER_THAN_EQ:
                        ctx.ltOrGt.computeIfAbsent(col, i -> new ArrayList<>()).add(new ColumnCondition(w.kind, eval(w.rhs)));
                        break;
                    //TODO (coverage): NOT_EQUAL
                    default:
                        throw new UnsupportedOperationException(w.kind.name());
                }
            }
            else if (w.lhs instanceof FunctionCall)
            {
                FunctionCall fn = (FunctionCall) w.lhs;
                switch (fn.name())
                {
                    case "token":
                        FunctionCall rhs = (FunctionCall) w.rhs;
                        List<ByteBuffer> pkValues = rhs.arguments.stream().map(ASTSingleTableModel::eval).collect(Collectors.toList());
                        BytesPartitionState.Ref ref = factory.createRef(new BufferClustering(pkValues.toArray(ByteBuffer[]::new)));
                        switch (w.kind)
                        {
                            case EQUAL:
                                ctx.token = ref.token;
                                break;
                            case LESS_THAN:
                            case LESS_THAN_EQ:
                                ctx.tokenUpperBound = new TokenCondition(w.kind, ref.token);
                                break;
                            case GREATER_THAN:
                            case GREATER_THAN_EQ:
                                ctx.tokenLowerBound = new TokenCondition(w.kind, ref.token);
                                break;
                            default:
                                throw new UnsupportedOperationException(w.kind.name());
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(fn.toCQL());
                }
            }
            else
            {
                throw new UnsupportedOperationException(w.lhs.getClass().getCanonicalName());
            }
        }
        else if (conditional instanceof Conditional.In)
        {
            Conditional.In in = (Conditional.In) conditional;
            if (in.ref instanceof Symbol)
            {
                Symbol col = (Symbol) in.ref;
                var override = ctx.eq.put(col, in.expressions);
                if (override != null)
                    throw new IllegalStateException("Column " + col.detailedName() + " had 2 '=' statements...");
                //TODO (correctness): can't find any documentation saying clustering is ordered by the data... it "could" but is it garanateed?
                if (factory.partitionColumns.contains(col) || factory.clusteringColumns.contains(col))
                    ctx.unordered = true;
            }
            else
            {
                throw new UnsupportedOperationException(in.ref.getClass().getCanonicalName());
            }
        }
        else if (conditional instanceof Conditional.Between)
        {
            Conditional.Between between = (Conditional.Between) conditional;
            if (between.ref instanceof Symbol)
            {
                Symbol col = (Symbol) between.ref;
                List<ColumnCondition> list = ctx.ltOrGt.computeIfAbsent(col, i -> new ArrayList<>());
                list.add(new ColumnCondition(Conditional.Where.Inequality.GREATER_THAN_EQ, eval(between.start)));
                list.add(new ColumnCondition(Conditional.Where.Inequality.LESS_THAN_EQ, eval(between.end)));
            }
            else if (between.ref instanceof FunctionCall)
            {
                FunctionCall fn = (FunctionCall) between.ref;
                switch (fn.name())
                {
                    case "token":
                        // if the ref is a token, the only valid start/end are also token
                        List<ByteBuffer> start = ((FunctionCall) between.start).arguments.stream().map(ASTSingleTableModel::eval).collect(Collectors.toList());
                        Token startToken = factory.createRef(new BufferClustering(start.toArray(ByteBuffer[]::new))).token;

                        List<ByteBuffer> end = ((FunctionCall) between.end).arguments.stream().map(ASTSingleTableModel::eval).collect(Collectors.toList());
                        Token endToken = factory.createRef(new BufferClustering(end.toArray(ByteBuffer[]::new))).token;

                        if (startToken.equals(endToken))
                        {
                            ctx.token = startToken;
                        }
                        else if (startToken.compareTo(endToken) > 0)
                        {
                            // start is larger than end... no matches
                            ctx.unmatchable = true;
                        }
                        else
                        {
                            ctx.tokenLowerBound = new TokenCondition(Conditional.Where.Inequality.GREATER_THAN_EQ, startToken);
                            ctx.tokenUpperBound = new TokenCondition(Conditional.Where.Inequality.LESS_THAN_EQ, endToken);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException(fn.toCQL());
                }
            }
            else
            {
                throw new UnsupportedOperationException(between.ref.getClass().getCanonicalName());
            }
        }
        else if (conditional instanceof Conditional.And)
        {
            Conditional.And and = (Conditional.And) conditional;
            context(ctx, and.left);
            context(ctx, and.right);
        }
        else
        {
            //TODO (coverage): IS
            throw new UnsupportedOperationException(conditional.getClass().getCanonicalName());
        }
    }

    private List<BytesPartitionState.PrimaryKey> search(LookupContext ctx)
    {
        // find by eq first
        Set<BytesPartitionState.PrimaryKey> eqMatches = searchEq(ctx);
        Set<BytesPartitionState.PrimaryKey> rangeMatches = searchRange(ctx);
        return new ArrayList<>(new TreeSet<>(intersectionEmptySafe(eqMatches, rangeMatches)));
    }

    private static <T> Set<T> intersectionEmptySafe(Set<T> a, Set<T> b)
    {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        return new HashSet<>(Sets.intersection(a, b));
    }

    private Set<BytesPartitionState.PrimaryKey> searchRange(LookupContext ctx)
    {
        Set<BytesPartitionState.PrimaryKey> matches = null;
        for (Map.Entry<Symbol, List<ColumnCondition>> e : ctx.ltOrGt.entrySet())
        {
            if (matches == null)
                matches = new HashSet<>(searchRange(e.getKey(), e.getValue()));
            else
            {
                boolean hadMatches = !matches.isEmpty();
                matches = new HashSet<>(intersectionEmptySafe(matches, new HashSet<>(searchRange(e.getKey(), e.getValue()))));
                if (hadMatches && matches.isEmpty())
                    return Collections.emptySet();
            }
        }
        return matches == null ? Collections.emptySet() : matches;
    }

    private List<BytesPartitionState.PrimaryKey> searchRange(Symbol symbol, List<ColumnCondition> conditions)
    {
        List<BytesPartitionState.PrimaryKey> matches = new ArrayList<>();
        if (factory.partitionColumns.contains(symbol) || factory.staticColumns.contains(symbol))
        {
            int pkOffset = factory.partitionColumns.indexOf(symbol);
            int sOffset = factory.staticColumns.indexOf(symbol);
            for (BytesPartitionState p : partitions.values())
            {
                if (pkOffset != -1)
                {
                    ByteBuffer value = p.key.bufferAt(pkOffset);
                    if (matches(symbol.type(), value, conditions))
                    {
                        if (p.isEmpty())
                        {
                            matches.add(p.partitionRowRef());
                        }
                        else
                        {
                            p.rows().forEach(r -> matches.add(r.ref()));
                        }
                    }
                }
                else
                {
                    // mutable columns, so may be null
                    ByteBuffer value = p.staticRow().get(sOffset);
                    if (value == null) continue;
                    if (matches(symbol.type(), value, conditions))
                    {
                        if (p.isEmpty())
                        {
                            matches.add(p.partitionRowRef());
                        }
                        else
                        {
                            p.rows().forEach(r -> matches.add(r.ref()));
                        }
                    }
                }
            }
        }
        else
        {
            int ckOffset = factory.clusteringColumns.indexOf(symbol);
            int rOffset = factory.regularColumns.indexOf(symbol);
            for (BytesPartitionState p : partitions.values())
            {
                for (BytesPartitionState.Row row : p.rows())
                {
                    if (ckOffset != -1)
                    {
                        ByteBuffer value = row.clustering.bufferAt(ckOffset);
                        if (matches(symbol.type(), value, conditions))
                            matches.add(row.ref());
                    }
                    else
                    {
                        // mutable columns, so may be null
                        var value = row.get(rOffset);
                        if (value == null) continue;
                        if (matches(symbol.type(), value, conditions))
                            matches.add(row.ref());
                    }
                }
            }
        }
        return matches;
    }

    private static boolean matches(AbstractType<?> type, ByteBuffer value, List<ColumnCondition> conditions)
    {
        for (ColumnCondition c : conditions)
        {
            int rc = type.compare(value, c.value);
            switch (c.inequality)
            {
                case LESS_THAN:
                    if (rc >= 0) return false;
                    break;
                case LESS_THAN_EQ:
                    if (rc > 0) return false;
                    break;
                case GREATER_THAN:
                    if (rc <= 0) return false;
                    break;
                case GREATER_THAN_EQ:
                    if (rc < 0) return false;
                    break;
                default:
                    throw new UnsupportedOperationException(c.inequality.name());
            }
        }
        return true;
    }

    /**
     * The common case there can only be 1 value, but in the case of {@link Conditional.In} this can be multiple.  When
     * multiple values are found then the semantic is OR rather than AND like the other matches function {@link #matches(AbstractType, ByteBuffer, List)}
     */
    private static boolean matches(ByteBuffer value, List<? extends Expression> conditions)
    {
        for (Expression e : conditions)
        {
            ByteBuffer expected = eval(e);
            if (expected.equals(value))
                return true;
        }
        return false;
    }

    private Set<BytesPartitionState.PrimaryKey> searchEq(LookupContext ctx)
    {
        Set<BytesPartitionState.PrimaryKey> matches = null;
        for (Map.Entry<Symbol, List<? extends Expression>> e : ctx.eq.entrySet())
        {
            for (Expression e2 : e.getValue())
            {
                ByteBuffer bb = eval(e2);
                if (matches == null)
                    matches = new HashSet<>(searchEq(e.getKey(), bb));
                else
                {
                    boolean hadMatches = !matches.isEmpty();
                    matches = new HashSet<>(intersectionEmptySafe(matches, new HashSet<>(searchEq(e.getKey(), bb))));
                    if (hadMatches && matches.isEmpty())
                        return Collections.emptySet();
                }
            }
        }
        return matches == null ? Collections.emptySet() : matches;
    }

    private List<BytesPartitionState.PrimaryKey> searchEq(Symbol symbol, ByteBuffer bb)
    {
        List<BytesPartitionState.PrimaryKey> matches = new ArrayList<>();
        if (factory.partitionColumns.contains(symbol) || factory.staticColumns.contains(symbol))
        {
            int pkOffset = factory.partitionColumns.indexOf(symbol);
            int sOffset = factory.staticColumns.indexOf(symbol);
            for (BytesPartitionState p : partitions.values())
            {
                if (pkOffset != -1)
                {
                    if (p.key.bufferAt(pkOffset).equals(bb))
                        p.rows().forEach(r -> matches.add(r.ref()));
                }
                else
                {
                    // mutable columns, so may be null
                    ByteBuffer value = p.staticRow().get(sOffset);
                    if (value == null) continue;
                    if (value.equals(bb))
                        p.rows().forEach(r -> matches.add(r.ref()));
                }
            }
        }
        else
        {
            int ckOffset = factory.clusteringColumns.indexOf(symbol);
            int rOffset = factory.regularColumns.indexOf(symbol);
            for (BytesPartitionState p : partitions.values())
            {
                for (BytesPartitionState.Row row : p.rows())
                {
                    if (ckOffset != -1)
                    {
                        if (row.clustering.bufferAt(ckOffset).equals(bb))
                            matches.add(row.ref());
                    }
                    else
                    {
                        // mutable columns, so may be null
                        var value = row.get(rOffset);
                        if (value == null) continue;
                        if (value.equals(bb))
                            matches.add(row.ref());
                    }
                }
            }
        }
        return matches;
    }

    private List<BytesPartitionState.PrimaryKey> findKeysByToken(LookupContext ctx)
    {
        return filter(ctx, getByToken(ctx.token));
    }

    private List<BytesPartitionState.PrimaryKey> findKeysByTokenSearch(LookupContext ctx)
    {
        return filter(ctx, getByTokenSearch(ctx.tokenLowerBound, ctx.tokenUpperBound));
    }

    private List<BytesPartitionState> getByTokenSearch(@Nullable TokenCondition tokenLowerBound,
                                                       @Nullable TokenCondition tokenUpperBound)
    {
        if (tokenLowerBound == null && tokenUpperBound == null)
            throw new IllegalArgumentException("At least one bound must be defined...");
        NavigableSet<BytesPartitionState.Ref> keys = partitions.navigableKeySet();
        // To support the case where 2+ keys share the same token, need to create a token ref before and after the token, to make sure
        // the head/tail sets find the matches correctly
        if (tokenLowerBound != null)
        {
            boolean inclusive;
            switch (tokenLowerBound.inequality)
            {
                case GREATER_THAN:
                    inclusive = false;
                    break;
                case GREATER_THAN_EQ:
                    inclusive = true;
                    break;
                default:
                    throw new UnsupportedOperationException(tokenLowerBound.inequality.name());
            }
            // when inclusive=true the ref should be before the token, that way the tokens match
            // when inclusive=false the ref should be after the token, that way they are excluded
            keys = keys.tailSet(factory.createRef(tokenLowerBound.token, !inclusive), inclusive);
        }
        if (tokenUpperBound != null)
        {
            boolean inclusive;
            switch (tokenUpperBound.inequality)
            {
                case LESS_THAN:
                    inclusive = false;
                    break;
                case LESS_THAN_EQ:
                    inclusive = true;
                    break;
                default:
                    throw new UnsupportedOperationException(tokenUpperBound.inequality.name());
            }
            // when inclusive=true the ref should be after the token
            // when inclusive=false the ref should be before the token
            keys = keys.headSet(factory.createRef(tokenUpperBound.token, inclusive), false);
        }
        if (keys.isEmpty()) return Collections.emptyList();
        return keys.stream().map(partitions::get).collect(Collectors.toList());
    }

    private List<BytesPartitionState.PrimaryKey> filter(LookupContext ctx, List<BytesPartitionState> partitions)
    {
        if (partitions.isEmpty()) return Collections.emptyList();
        List<BytesPartitionState.PrimaryKey> matches = new ArrayList<>();
        for (BytesPartitionState p : partitions)
            matches.addAll(filter(ctx, p));
        return matches;
    }

    private List<BytesPartitionState.PrimaryKey> filter(LookupContext ctx, BytesPartitionState partition)
    {
        Map<Symbol, List<? extends Expression>> values = ctx.eq;
        List<BytesPartitionState.PrimaryKey> rows = new ArrayList<>(partition.size());
        if (!factory.clusteringColumns.isEmpty() && values.keySet().containsAll(factory.clusteringColumns))
        {
            // single row
            for (Clustering<ByteBuffer> cd : keys(values, factory.clusteringColumns))
            {
                BytesPartitionState.Row row = partition.get(cd);
                if (row != null && ctx.include(row))
                    rows.add(row.ref());
            }
        }
        else
        {
            // full partition
            if (partition.isEmpty())
            {
                //TODO (now, correctness): if you query a non-partition column and this is empty, this condition isn't true...
                rows.add(partition.partitionRowRef());
            }
            else
            {
                for (BytesPartitionState.Row row : partition.rows())
                {
                    if (ctx.include(row))
                        rows.add(row.ref());
                }
            }
        }
        return rows;
    }

    private List<BytesPartitionState.PrimaryKey> findByPartitionEq(LookupContext ctx)
    {
        List<BytesPartitionState.PrimaryKey> matches = new ArrayList<>();
        for (Clustering<ByteBuffer> pd : keys(ctx.eq, factory.partitionColumns))
        {
            BytesPartitionState partition = partitions.get(factory.createRef(pd));
            if (partition == null || !ctx.include(partition)) continue;
            matches.addAll(filter(ctx, partition));
        }
        return matches;
    }

    private Clustering<ByteBuffer> key(Map<Symbol, Expression> values, ImmutableUniqueList<Symbol> columns)
    {
        // same as keys, but only one possible value can happen
        List<Clustering<ByteBuffer>> keys = keys(Maps.transformValues(values, e -> Collections.singletonList(e)), columns);
        Invariants.checkState(keys.size() == 1, "Expected 1 key, but found %d", keys.size());
        return keys.get(0);
    }

    private List<Clustering<ByteBuffer>> keys(Map<Symbol, List<? extends Expression>> values, ImmutableUniqueList<Symbol> columns)
    {
        if (columns.isEmpty()) return Collections.singletonList(Clustering.EMPTY);
        List<ByteBuffer[]> current = new ArrayList<>();
        current.add(new ByteBuffer[columns.size()]);
        for (Symbol symbol : columns)
        {
            int position = columns.indexOf(symbol);
            List<? extends Expression> expressions = values.get(symbol);
            ByteBuffer firstBB = eval(expressions.get(0));
            current.forEach(bbs -> bbs[position] = firstBB);
            if (expressions.size() > 1)
            {
                // this has a multiplying effect... if there is 1 row and there are 2 expressions, then we have 2 rows
                // if there are 2 rows and 2 expressions, we have 4 rows... and so on...
                List<ByteBuffer[]> copy = new ArrayList<>(current);
                for (int i = 1; i < expressions.size(); i++)
                {
                    ByteBuffer bb = eval(expressions.get(i));
                    for (ByteBuffer[] bbs : copy)
                    {
                        bbs = bbs.clone();
                        bbs[position] = bb;
                        current.add(bbs);
                    }
                }
            }
        }
        return current.stream().map(BufferClustering::new).collect(Collectors.toList());
    }

    private static ByteBuffer eval(Expression e)
    {
        return ExpressionEvaluator.tryEvalEncoded(e).get();
    }

    private static class Row
    {
        private final ImmutableUniqueList<Symbol> columns;
        private final ByteBuffer[] values;

        private Row(ImmutableUniqueList<Symbol> columns, ByteBuffer[] values)
        {
            this.columns = columns;
            this.values = values;
        }

        public String asCQL(Symbol symbol)
        {
            int offset = columns.indexOf(symbol);
            assert offset >= 0;
            ByteBuffer b = values[offset];
            return (b == null || ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b)) ? "null" : symbol.type().asCQL3Type().toCQLLiteral(b);
        }

        public List<String> asCQL()
        {
            List<String> human = new ArrayList<>(values.length);
            for (int i = 0; i < values.length; i++)
                human.add(asCQL(columns.get(i)));
            return human;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Row row = (Row) o;
            return Arrays.equals(values, row.values);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(values);
        }

        @Override
        public String toString()
        {
            return asCQL().toString();
        }
    }

    private class LookupContext
    {
        private final Select select;
        private Map<Symbol, List<? extends Expression>> eq = new HashMap<>();
        private final Map<Symbol, List<ColumnCondition>> ltOrGt = new HashMap<>();
        @Nullable
        private Token token = null;
        @Nullable
        private TokenCondition tokenLowerBound, tokenUpperBound;
        private boolean unordered = false;
        private boolean unmatchable = false;

        private LookupContext(Select select)
        {
            this.select = select;
        }

        boolean include(BytesPartitionState partition)
        {
            if (unmatchable) return false;
            if (!include(factory.partitionColumns, partition.key::bufferAt))
                return false;
            if (!factory.staticColumns.isEmpty()
                && !include(factory.staticColumns, partition.staticRow()::get))
                return false;
            return true;
        }

        boolean include(BytesPartitionState.Row row)
        {
            if (unmatchable) return false;
            if (!factory.clusteringColumns.isEmpty()
                && !include(factory.clusteringColumns, row.clustering::bufferAt))
                return false;
            if (!factory.regularColumns.isEmpty()
                && !include(factory.regularColumns, row::get))
                return false;
            return true;
        }

        private boolean include(ImmutableUniqueList<Symbol> columns, IntFunction<ByteBuffer> accessor)
        {
            for (Symbol col : columns)
            {
                if (eq.containsKey(col))
                {
                    ByteBuffer actual = accessor.apply(columns.indexOf(col));
                    if (!matches(actual, eq.get(col)))
                        return false;
                }
                if (ltOrGt.containsKey(col))
                {
                    ByteBuffer actual = accessor.apply(columns.indexOf(col));
                    if (!matches(col.type(), actual, ltOrGt.get(col)))
                        return false;
                }
            }
            return true;
        }
    }

    private static class ColumnCondition
    {
        private final Conditional.Where.Inequality inequality;
        private final ByteBuffer value;

        private ColumnCondition(Conditional.Where.Inequality inequality, ByteBuffer value)
        {
            this.inequality = inequality;
            this.value = value;
        }
    }

    private static class TokenCondition
    {
        private final Conditional.Where.Inequality inequality;
        private final Token token;

        private TokenCondition(Conditional.Where.Inequality inequality, Token token)
        {
            this.inequality = inequality;
            this.token = token;
        }
    }
}
