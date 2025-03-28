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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import accord.utils.Invariants;
import org.apache.cassandra.cql3.ast.AssignmentOperator;
import org.apache.cassandra.cql3.ast.CasCondition;
import org.apache.cassandra.cql3.ast.Conditional;
import org.apache.cassandra.cql3.ast.Conditional.Where.Inequality;
import org.apache.cassandra.cql3.ast.Element;
import org.apache.cassandra.cql3.ast.Expression;
import org.apache.cassandra.cql3.ast.ExpressionEvaluator;
import org.apache.cassandra.cql3.ast.FunctionCall;
import org.apache.cassandra.cql3.ast.Literal;
import org.apache.cassandra.cql3.ast.Mutation;
import org.apache.cassandra.cql3.ast.Operator;
import org.apache.cassandra.cql3.ast.Reference;
import org.apache.cassandra.cql3.ast.ReferenceExpression;
import org.apache.cassandra.cql3.ast.Select;
import org.apache.cassandra.cql3.ast.StandardVisitors;
import org.apache.cassandra.cql3.ast.Symbol;
import org.apache.cassandra.cql3.ast.Value;
import org.apache.cassandra.cql3.ast.Visitor;
import org.apache.cassandra.db.BufferClustering;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.harry.model.BytesPartitionState.PrimaryKey;
import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ImmutableUniqueList;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.harry.model.BytesPartitionState.asCQL;

public class ASTSingleTableModel
{
    private static final ByteBuffer[][] NO_ROWS = new ByteBuffer[0][];

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

    public TreeMap<ByteBuffer, List<PrimaryKey>> index(BytesPartitionState.Ref ref, Symbol symbol)
    {
        if (factory.partitionColumns.contains(symbol))
            throw new AssertionError("When indexing based off a single partition, unable to index partition columns; given " + symbol.detailedName());
        BytesPartitionState partition = get(ref);
        Invariants.nonNull(partition, "Unable to index %s; null partition %s", symbol, ref);
        TreeMap<ByteBuffer, List<PrimaryKey>> index = new TreeMap<>(symbol.type());
        if (factory.staticColumns.contains(symbol))
            return indexStaticColumn(index, symbol, partition);
        return indexRowColumn(index, symbol, partition);
    }

    public TreeMap<ByteBuffer, List<PrimaryKey>> index(Symbol symbol)
    {
        TreeMap<ByteBuffer, List<PrimaryKey>> index = new TreeMap<>(symbol.type());
        if (factory.partitionColumns.contains(symbol))
            return indexPartitionColumn(index, symbol);
        if (factory.staticColumns.contains(symbol))
            return indexStaticColumn(index, symbol);
        return indexRowColumn(index, symbol);
    }

    private TreeMap<ByteBuffer, List<PrimaryKey>> indexPartitionColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, Symbol symbol)
    {
        int offset = factory.partitionColumns.indexOf(symbol);
        for (BytesPartitionState partition : partitions.values())
        {
            if (partition.isEmpty()) continue;
            ByteBuffer bb = partition.key.bufferAt(offset);
            List<PrimaryKey> list = index.computeIfAbsent(bb, i -> new ArrayList<>());
            for (BytesPartitionState.Row row : partition.rows())
                list.add(row.ref());
        }
        return index;
    }

    private TreeMap<ByteBuffer, List<PrimaryKey>> indexStaticColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, Symbol symbol)
    {
        for (BytesPartitionState partition : partitions.values())
            indexStaticColumn(index, symbol, partition);
        return index;
    }

    private TreeMap<ByteBuffer, List<PrimaryKey>> indexStaticColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, Symbol symbol, BytesPartitionState partition)
    {
        if (partition.isEmpty()) return index;
        ByteBuffer bb = partition.staticRow().get(symbol);
        if (bb == null)
            return index;
        List<PrimaryKey> list = index.computeIfAbsent(bb, i -> new ArrayList<>());
        for (BytesPartitionState.Row row : partition.rows())
            list.add(row.ref());
        return index;
    }

    private TreeMap<ByteBuffer, List<PrimaryKey>> indexRowColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, Symbol symbol)
    {
        boolean clustering = factory.clusteringColumns.contains(symbol);
        int offset = clustering ? factory.clusteringColumns.indexOf(symbol) : factory.regularColumns.indexOf(symbol);
        for (BytesPartitionState partition : partitions.values())
            indexRowColumn(index, clustering, offset, partition);
        return index;
    }

    private TreeMap<ByteBuffer, List<PrimaryKey>> indexRowColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, Symbol symbol, BytesPartitionState partition)
    {
        boolean clustering = factory.clusteringColumns.contains(symbol);
        int offset = clustering ? factory.clusteringColumns.indexOf(symbol) : factory.regularColumns.indexOf(symbol);
        indexRowColumn(index, clustering, offset, partition);
        return index;
    }

    private void indexRowColumn(TreeMap<ByteBuffer, List<PrimaryKey>> index, boolean clustering, int offset, BytesPartitionState partition)
    {
        if (partition.isEmpty()) return;
        for (BytesPartitionState.Row row : partition.rows())
        {
            ByteBuffer bb = clustering ? row.clustering.bufferAt(offset) : row.get(offset);
            if (bb == null)
                continue;
            index.computeIfAbsent(bb, i -> new ArrayList<>()).add(row.ref());
        }
    }

    public void update(Mutation mutation)
    {
        if (!shouldApply(mutation)) return;
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

    private void update(Mutation.Insert insert)
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
        // table has clustering but non are in the write, so only pk/static can be updated
        if (!factory.clusteringColumns.isEmpty() && Sets.intersection(factory.clusteringColumns.asSet(), values.keySet()).isEmpty())
            return;
        Map<Symbol, ByteBuffer> write = new HashMap<>();
        for (Symbol col : Sets.intersection(factory.regularColumns.asSet(), values.keySet()))
            write.put(col, eval(values.get(col)));
        partition.setColumns(key(insert.values, factory.clusteringColumns),
                             write,
                             true);
    }

    private void update(Mutation.Update update)
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
                {
                    ByteBuffer current = partition.staticRow().get(col);
                    EvalResult result = eval(col, current, set.get(col));
                    if (result.kind == EvalResult.Kind.SKIP) continue;
                    write.put(col, result.value);
                }
                if (!write.isEmpty())
                    partition.setStaticColumns(write);
            }
            // table has clustering but non are in the write, so only pk/static can be updated
            if (!factory.clusteringColumns.isEmpty() && remaining.isEmpty())
                return;
            for (Clustering<ByteBuffer> cd : clustering(remaining))
            {
                Map<Symbol, ByteBuffer> write = new HashMap<>();
                for (Symbol col : Sets.intersection(factory.regularColumns.asSet(), set.keySet()))
                {
                    ByteBuffer current = partition.get(cd, col);
                    EvalResult result = eval(col, current, set.get(col));
                    if (result.kind == EvalResult.Kind.SKIP) continue;
                    write.put(col, result.value);
                }

                if (!write.isEmpty())
                    partition.setColumns(cd, write, false);
            }
        }
    }

    private enum DeleteKind
    {PARTITION, ROW, COLUMN}

    private void update(Mutation.Delete delete)
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
                        if (partition.shouldDelete())
                            partitions.remove(partition.ref());
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
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public boolean shouldApply(Mutation mutation)
    {
        if (!mutation.isCas()) return true;
        return shouldApply(mutation, selectPartitionForCAS(mutation));
    }

    private SelectResult selectPartitionForCAS(Mutation mutation)
    {
        var partition = partitions.get(factory.createRef(pd(mutation)));
        if (partition == null) return SelectResult.ordered(factory.selectionOrder, NO_ROWS);

        var cd = cdOrNull(mutation);
        var row = cd == null ? null : partition.get(cd);
        ImmutableUniqueList<Symbol> columns = cd != null ? factory.selectionOrder : factory.partitionAndStaticColumns;
        return SelectResult.ordered(columns, new ByteBuffer[][] { getRowAsByteBuffer(columns, partition, row)});
    }

    private boolean shouldApply(Mutation mutation, SelectResult current)
    {
        Preconditions.checkArgument(mutation.isCas());
        // process condition
        CasCondition condition;
        switch (mutation.kind)
        {
            case INSERT:
                condition = CasCondition.Simple.NotExists;
                break;
            case UPDATE:
                condition = ((Mutation.Update) mutation).casCondition.get();
                break;
            case DELETE:
                condition = ((Mutation.Delete) mutation).casCondition.get();
                break;
            default:
                throw new UnsupportedOperationException(mutation.kind.name());
        }
        if (condition instanceof CasCondition.Simple)
        {
            boolean hasPartition = current.rows.length > 0;
            boolean partitionOrRow = current.columns.equals(factory.partitionAndStaticColumns);
            boolean hasRow = partitionOrRow ? hasPartition : current.isAllDefined(factory.clusteringColumns);
            var simple = (CasCondition.Simple) condition;
            switch (simple)
            {
                case Exists:
                    return hasRow;
                case NotExists:
                    return !hasRow;
                default:
                    throw new UnsupportedOperationException(simple.name());
            }
        }
        var ifCondition = (CasCondition.IfCondition) condition;
        String letRow = "row";
        Symbol rowSymbol = Symbol.unknownType(letRow);
        Map<String, SelectResult> lets = Map.of(letRow, current);
        // point the columns to be row.column that way it matches LET clause in BEGIN TRANSACTION, allowing better reuse
        var updatedCondition = ifCondition.conditional.visit(new Visitor()
        {
            @Override
            public ReferenceExpression visit(ReferenceExpression r)
            {
                Preconditions.checkArgument(!(r instanceof Reference), "Unexpected reference detected: %s", r);
                return Reference.of(rowSymbol, r);
            }
        });
        return process(updatedCondition, lets);
    }

    private boolean process(Conditional condition, Map<String, SelectResult> lets)
    {
        if (condition.getClass() == Conditional.Is.class)
        {
            var is = (Conditional.Is) condition;
            Object result = extract(is.reference, lets);
            return result == null
                   ? is.kind == Conditional.Is.Kind.Null
                   : is.kind == Conditional.Is.Kind.NotNull;
        }
        else if (condition.getClass() == Conditional.Where.class)
        {
            var where = (Conditional.Where) (condition);
            if (!where.lhs.type().equals(where.rhs.type()))
                throw new UnsupportedOperationException("For now where clause must always have matching types: given " + where.lhs.type() + ' ' + where.rhs.type());
            ByteBuffer lhs = where.lhs instanceof ReferenceExpression
                             ? (ByteBuffer) extract((ReferenceExpression) where.lhs, lets)
                             : eval(where.lhs);
            ByteBuffer rhs = where.rhs instanceof ReferenceExpression
                             ? (ByteBuffer) extract((ReferenceExpression) where.rhs, lets)
                             : eval(where.rhs);
            // If anything is null avoid doing the test, but there is a special case where this returns true... both sides are null!
            // This logic isn't consistent with other parts of the database and is local to CAS IF clause
            // see ML@Inconsistent null handling between WHERE and IF clauses
            if (lhs == null || rhs == null)
                return lhs == rhs;
            return where.kind.test(where.lhs.type(), lhs, rhs);
        }
        else if (condition.getClass() == Conditional.And.class)
        {
            var conditions = condition.simplify();
            for (var c : conditions)
            {
                if (!process(c, lets))
                    return false;
            }
            return true;
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported condition type: " + condition.getClass() + "; " + condition.toCQL());
        }
    }

    // Either ByteBuffer (cell) or ByteBuffer[] (row)
    private static Object extract(ReferenceExpression expr, Map<String, SelectResult> lets)
    {
        Object result = extract0(expr, lets);
        if (result instanceof SelectResult)
        {
            var rows = ((SelectResult) result).rows;
            result = rows.length == 0 ? null : rows[0];
        }
        return result;
    }

    // o can be Map<String, SelectResult> (lets), SelectResult (row), ByteBuffer (cell)
    private static Object extract0(ReferenceExpression expr, @Nullable Object o)
    {
        if (o == null) return null;
        if (expr instanceof Reference)
        {
            Reference ref = (Reference) expr;
            for (var symbol : ref.path)
                o = extract0(symbol, o);
            return o;
        }
        else if (expr instanceof Symbol)
        {
            var symbol = (Symbol) expr;
            if (o instanceof Map)
            {
                Map<String, SelectResult> lets = (Map<String, SelectResult>) o;
                return lets.get(symbol.symbol);
            }
            else if (o instanceof SelectResult)
            {
                SelectResult result = (SelectResult) o;
                if (result.rows.length == 0)
                    return null;
                return result.rows[0][result.columns.indexOf(symbol)];
            }
            else
            {
                throw new UnsupportedOperationException("Unexpected object type: " + o.getClass());
            }
        }
        else
        {
            throw new UnsupportedOperationException("Unsupported ref type: " + expr.getClass() + "; " + expr.toCQL());
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
                if (w.kind == Inequality.EQUAL && columns.contains(w.lhs))
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

    private Clustering<ByteBuffer> pd(Mutation mutation)
    {
        switch (mutation.kind)
        {
            case INSERT:
                return pd((Mutation.Insert) mutation);
            case UPDATE:
                return pd((Mutation.Update) mutation);
            case DELETE:
                return pd((Mutation.Delete) mutation);
            default:
                throw new UnsupportedOperationException(mutation.kind.name());
        }
    }

    private Clustering<ByteBuffer> pd(Mutation.Insert mutation)
    {
        return key(mutation.values, factory.partitionColumns);
    }

    private Clustering<ByteBuffer> pd(Mutation.Update mutation)
    {
        return pd("Update", mutation.where.simplify());
    }

    private Clustering<ByteBuffer> pd(Mutation.Delete mutation)
    {
        return pd("Delete", mutation.where.simplify());
    }

    private Clustering<ByteBuffer> pd(String type, List<Conditional> conditionals)
    {
        var split = splitOnPartition(conditionals);
        List<Clustering<ByteBuffer>> pks = split.left;
        Preconditions.checkArgument(pks.size() == 1, "%s had more than 1 partition key!  expected 1 but was %s", type, pks.size());
        return pks.get(0);
    }

    @Nullable
    private Clustering<ByteBuffer> cdOrNull(Mutation mutation)
    {
        if (factory.clusteringColumns.isEmpty()) return Clustering.EMPTY;
        if (mutation.kind == Mutation.Kind.INSERT)
        {
            var insert = (Mutation.Insert) mutation;
            return !insert.values.keySet().containsAll(factory.clusteringColumns)
                   ? null
                   : key(insert.values, factory.clusteringColumns);
        }
        Conditional where;
        switch (mutation.kind)
        {
            case UPDATE:
                where = ((Mutation.Update) mutation).where;
                break;
            case DELETE:
                where = ((Mutation.Delete) mutation).where;
            break;
            default:
                throw new UnsupportedOperationException("Unexpected mutation: " + mutation.kind);
        }
        var partitions = splitOnPartition(where.simplify());
        if (partitions.right.isEmpty()) return null;
        var matches = clustering(partitions.right);
        Preconditions.checkArgument(matches.size() == 1);
        return matches.get(0);
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
        try
        {
            if (results.unordered)
            {
                validateAnyOrder(factory.selectionOrder, toRow(factory.selectionOrder, actual), toRow(factory.selectionOrder, results.rows));
            }
            else
            {
                validate(actual, results.rows);
            }
        }
        catch (AssertionError e)
        {
            AssertionError error = new AssertionError("Unexpected results for query: " + StringUtils.escapeControlChars(select.visit(StandardVisitors.DEBUG).toCQL()), e);
            // This stack trace is not helpful, this error message is trying to improve the error returned to know what query failed, so the stack trace only adds noise
            error.setStackTrace(new StackTraceElement[0]);
            throw error;
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
        // all rows match, but are they in the right order?
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
        if (!unexpected.isEmpty() && unexpected.size() == missing.size())
        {
            // good chance a column differs
            StringBuilder finalSb = sb;
            Runnable runOnce = new Runnable()
            {
                boolean ran = false;
                @Override
                public void run()
                {
                    if (ran) return;
                    finalSb.append("\nPossible column conflicts:");
                    ran = true;
                }
            };
            for (var e : missing)
            {
                Row smallest = null;
                BitSet smallestDiff = null;
                for (var a : unexpected)
                {
                    BitSet diff = e.diff(a);
                    if (smallestDiff == null || diff.cardinality() < smallestDiff.cardinality())
                    {
                        smallest = a;
                        smallestDiff = diff;
                    }
                }
                // if every column differs then ignore
                if (smallestDiff.cardinality() == e.values.length)
                    continue;
                runOnce.run();
                sb.append("\n\tExpected: ").append(e);
                sb.append("\n\tDiff (expected over actual):\n");
                Row eSmall = e.select(smallestDiff);
                Row aSmall = smallest.select(smallestDiff);
                sb.append(table(eSmall.columns, Arrays.asList(eSmall, aSmall)));
            }
        }
        if (sb != null)
        {
            sb.append("\nExpected:\n").append(table(columns, expected));
            throw new AssertionError(sb.toString());
        }
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
            sb.append("\nActual:\n").append(table(columns, actual));
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

    private static Set<Row> toRow(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows)
    {
        Set<Row> set = new HashSet<>();
        for (ByteBuffer[] row : rows)
            set.add(new Row(columns, row));
        return set;
    }

    private static class SelectResult
    {
        private final ImmutableUniqueList<Symbol> columns;
        private final ByteBuffer[][] rows;
        private final boolean unordered;

        private SelectResult(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows, boolean unordered)
        {
            this.columns = columns;
            this.rows = rows;
            this.unordered = unordered;
        }

        private static SelectResult ordered(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows)
        {
            return new SelectResult(columns, rows, false);
        }

        private static SelectResult unordered(ImmutableUniqueList<Symbol> columns, ByteBuffer[][] rows)
        {
            return new SelectResult(columns, rows, true);
        }

        public boolean isAllDefined(ImmutableUniqueList<Symbol> selectColumns)
        {
            if (rows.length == 0) return false;
            for (var row : rows)
            {
                for (var col : selectColumns)
                {
                    if (row[columns.indexOf(col)] == null)
                        return false;
                }
            }
            return true;
        }
    }

    public ImmutableUniqueList<Symbol> columns(Select select)
    {
        if (select.selections.isEmpty()) return factory.selectionOrder;
        throw new UnsupportedOperationException("Getting columns from select other than SELECT * is currently not supported");
    }

    private SelectResult getRowsAsByteBuffer(Select select)
    {
        ImmutableUniqueList<Symbol> columns = columns(select);
        if (select.where.isEmpty())
            return SelectResult.ordered(columns, getRowsAsByteBuffer(applyLimits(all(), select.perPartitionLimit, select.limit)));
        LookupContext ctx = context(select);
        List<PrimaryKey> primaryKeys;
        if (ctx.unmatchable)
        {
            primaryKeys = Collections.emptyList();
        }
        else if (ctx.eq.keySet().containsAll(factory.partitionColumns))
        {
            // tested
            primaryKeys = findByPartitionEq(ctx);
        }
        else if (ctx.token != null)
        {
            // tested
            primaryKeys = findKeysByToken(ctx);
        }
        else if (ctx.tokenLowerBound != null || ctx.tokenUpperBound != null)
        {
            primaryKeys = findKeysByTokenSearch(ctx);
        }
        else
        {
            // partial tested (handles many columns, tests are single column)
            primaryKeys = search(ctx);
        }
        primaryKeys = applyLimits(primaryKeys, select.perPartitionLimit, select.limit);
        //TODO (correctness): now that we have the rows we need to handle the selections/aggregation/limit/group-by/etc.
        return new SelectResult(columns, getRowsAsByteBuffer(primaryKeys), ctx.unordered);
    }

    private List<PrimaryKey> applyLimits(List<PrimaryKey> primaryKeys, Optional<Value> perPartitionLimitOpt, Optional<Value> limitOpt)
    {
        if (perPartitionLimitOpt.isPresent())
        {
            int limit = Int32Type.instance.compose(eval(perPartitionLimitOpt.get()));
            var it = primaryKeys.iterator();
            BytesPartitionState.Ref current = null;
            int count = 0;
            while (it.hasNext())
            {
                PrimaryKey next = it.next();
                if (current == null || !current.equals(next.partition))
                {
                    current = next.partition;
                    count = 0;
                }
                if (++count > limit)
                    it.remove();
            }
        }
        if (limitOpt.isPresent())
        {
            int limit = Int32Type.instance.compose(eval(limitOpt.get()));
            if (primaryKeys.size() > limit)
                primaryKeys = primaryKeys.subList(0, limit);
        }
        return primaryKeys;
    }

    private List<PrimaryKey> all()
    {
        List<PrimaryKey> primaryKeys = new ArrayList<>();
        for (var partition : partitions.values())
        {
            if (partition.staticOnly()) primaryKeys.add(partition.partitionRowRef());
            else partition.rows().stream().map(BytesPartitionState.Row::ref).forEach(primaryKeys::add);
        }
        return primaryKeys;
    }

    public ByteBuffer[][] getRowsAsByteBuffer(List<PrimaryKey> primaryKeys)
    {
        ByteBuffer[][] rows = new ByteBuffer[primaryKeys.size()][];
        int idx = 0;
        for (PrimaryKey pk : primaryKeys)
        {
            BytesPartitionState partition = partitions.get(pk.partition);
            BytesPartitionState.Row row = partition.get(pk.clustering);
            rows[idx++] = getRowAsByteBuffer(partition, row);
        }
        return rows;
    }

    private ByteBuffer[] getRowAsByteBuffer(BytesPartitionState partition, @Nullable BytesPartitionState.Row row)
    {
        return getRowAsByteBuffer(factory.selectionOrder, partition, row);
    }

    private ByteBuffer[] getRowAsByteBuffer(ImmutableUniqueList<Symbol> columns, BytesPartitionState partition, @Nullable BytesPartitionState.Row row)
    {
        Clustering<ByteBuffer> pd = partition.key;
        BytesPartitionState.Row staticRow = partition.staticRow();
        ByteBuffer[] bbs = new ByteBuffer[columns.size()];
        for (Symbol col : factory.partitionColumns)
        {
            if (!columns.contains(col)) continue;
            bbs[columns.indexOf(col)] = pd.bufferAt(factory.partitionColumns.indexOf(col));
        }
        for (Symbol col : factory.staticColumns)
        {
            if (!columns.contains(col)) continue;
            bbs[columns.indexOf(col)] = staticRow.get(col);
        }
        if (row != null)
        {
            for (Symbol col : factory.clusteringColumns)
            {
                if (!columns.contains(col)) continue;
                bbs[columns.indexOf(col)] = row.clustering.bufferAt(factory.clusteringColumns.indexOf(col));
            }
            for (Symbol col : factory.regularColumns)
            {
                if (!columns.contains(col)) continue;
                bbs[columns.indexOf(col)] = row.get(col);
            }
        }
        return bbs;
    }

    private LookupContext context(Select select)
    {
        if (select.where.isEmpty())
            throw new IllegalArgumentException("Select without a where clause was expected to be handled before this point");
        return new LookupContext(select);
    }

    private List<PrimaryKey> search(LookupContext ctx)
    {
        List<PrimaryKey> matches = new ArrayList<>();
        for (BytesPartitionState partition : partitions.values())
        {
            if (!ctx.include(partition)) continue;
            matches.addAll(filter(ctx, partition));
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
            if (expected != null && expected.equals(value))
                return true;
        }
        return false;
    }

    private List<PrimaryKey> findKeysByToken(LookupContext ctx)
    {
        return filter(ctx, getByToken(ctx.token));
    }

    private List<PrimaryKey> findKeysByTokenSearch(LookupContext ctx)
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

    private List<PrimaryKey> filter(LookupContext ctx, List<BytesPartitionState> partitions)
    {
        if (partitions.isEmpty()) return Collections.emptyList();
        List<PrimaryKey> matches = new ArrayList<>();
        for (BytesPartitionState p : partitions)
        {
            if (!ctx.include(p)) continue;
            matches.addAll(filter(ctx, p));
        }
        return matches;
    }

    private List<PrimaryKey> filter(LookupContext ctx, BytesPartitionState partition)
    {
        Map<Symbol, List<? extends Expression>> values = ctx.eq;
        List<PrimaryKey> rows = new ArrayList<>(partition.size());
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
                if (ctx.testsRow())
                    return Collections.emptyList();
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

    private List<PrimaryKey> findByPartitionEq(LookupContext ctx)
    {
        List<PrimaryKey> matches = new ArrayList<>();
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
        List<Clustering<ByteBuffer>> keys = keys(Maps.transformValues(values, Collections::singletonList), columns);
        Preconditions.checkState(keys.size() == 1, "Expected 1 key, but found %d", keys.size());
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

    private static class EvalResult
    {
        private static final EvalResult SKIP = new EvalResult(Kind.SKIP, null);

        private enum Kind { SKIP, ACCEPT }

        private final Kind kind;
        private final @Nullable ByteBuffer value;

        private EvalResult(Kind kind, @Nullable ByteBuffer value)
        {
            this.kind = kind;
            this.value = value;
        }

        private static EvalResult accept(@Nullable ByteBuffer bb)
        {
            return new EvalResult(Kind.ACCEPT, bb);
        }
    }

    private static EvalResult eval(Symbol col, @Nullable ByteBuffer current, Expression e)
    {
        if (!(e instanceof AssignmentOperator)) return EvalResult.accept(eval(e));
        current = col.type().sanitize(current);
        // multi cell collections have the property that they do update even if the current value is null
        boolean isFancy = col.type().isCollection() && col.type().isMultiCell();
        if (current == null && !isFancy) return EvalResult.SKIP; // null + ? == null
        var assignment = (AssignmentOperator) e;
        if (isFancy && current == null)
        {
            return assignment.kind == AssignmentOperator.Kind.SUBTRACT
                   // if it doesn't exist, then there is nothing to subtract
                   ? EvalResult.SKIP
                   : EvalResult.accept(eval(assignment.right));
        }
        // validate your inputs...
        ByteBuffer rhs = col.type().sanitize(eval(assignment.right));
        if (rhs == null)
            return EvalResult.SKIP;
        switch (assignment.kind)
        {
            case ADD:
                return EvalResult.accept(eval(new Operator(Operator.Kind.ADD, new Literal(current, e.type()), assignment.right)));
            case SUBTRACT:
                return EvalResult.accept(eval(new Operator(Operator.Kind.SUBTRACT, new Literal(current, e.type()), assignment.right)));
            default:
                throw new UnsupportedOperationException(assignment.kind + ": " + assignment.toCQL());
        }
    }

    @Nullable
    private static ByteBuffer eval(Expression e)
    {
        return ExpressionEvaluator.evalEncoded(e);
    }

    private static class Row
    {
        private static final Row EMPTY = new Row(ImmutableUniqueList.empty(), ByteBufferUtil.EMPTY_ARRAY);

        private final ImmutableUniqueList<Symbol> columns;
        private final ByteBuffer[] values;

        private Row(ImmutableUniqueList<Symbol> columns, ByteBuffer[] values)
        {
            this.columns = columns;
            this.values = values;
            if (columns.size() != values.length)
                throw new IllegalArgumentException("Columns " + columns + " should have the same size as values, but had " + values.length);
        }

        public String asCQL(Symbol symbol)
        {
            int offset = columns.indexOf(symbol);
            assert offset >= 0;
            ByteBuffer b = values[offset];
            if (b == null) return "null";
            if (ByteBufferUtil.EMPTY_BYTE_BUFFER.equals(b)) return "<empty>";
            return symbol.type().asCQL3Type().toCQLLiteral(b);
        }

        public List<String> asCQL()
        {
            List<String> human = new ArrayList<>(values.length);
            for (int i = 0; i < values.length; i++)
                human.add(asCQL(columns.get(i)));
            return human;
        }

        public BitSet diff(Row other)
        {
            if (!columns.equals(other.columns))
                throw new UnsupportedOperationException("Columns do not match: expected " + columns + " but given " + other.columns);
            int maxLength = Math.max(values.length, other.values.length);
            int minLength = Math.min(values.length, other.values.length);
            BitSet set = new BitSet(maxLength);
            for (int i = 0; i < minLength; i++)
            {
                ByteBuffer a = values[i];
                ByteBuffer b = other.values[i];
                if (!Objects.equals(a, b))
                    set.set(i);
            }
            for (int i = minLength; i < maxLength; i++)
                set.set(i);
            return set;
        }

        public Row select(BitSet selection)
        {
            if (selection.isEmpty()) return EMPTY;
            var names = ImmutableUniqueList.<Symbol>builder(selection.cardinality());
            ByteBuffer[] copy = new ByteBuffer[selection.cardinality()];
            int offset = 0;
            for (int i = 0; i < this.values.length; i++)
            {
                if (!selection.get(i)) continue;
                names.add(this.columns.get(i));
                copy[offset++] = this.values[i];
            }
            return new Row(names.build(), copy);
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
        private final Map<Symbol, List<? extends Expression>> eq = new HashMap<>();
        private final Map<Symbol, List<ColumnCondition>> ltOrGt = new HashMap<>();
        @Nullable
        private Token token = null;
        @Nullable
        private TokenCondition tokenLowerBound, tokenUpperBound;
        private boolean unordered = false;
        private boolean unmatchable = false;

        private LookupContext(Select select)
        {
            addConditional(select.where.get());
            maybeNormalizeTokenBounds();
        }

        private void maybeNormalizeTokenBounds()
        {
            if (tokenLowerBound != null && tokenUpperBound != null)
            {
                int rc = tokenLowerBound.token.compareTo(tokenUpperBound.token);
                if (rc > 0)
                {
                    // where token > 10 and < 0.... nothing matches that!
                    unmatchable = true;
                    tokenLowerBound = null;
                    tokenUpperBound = null;
                }
                else if (rc == 0)
                {
                    // tokens match... but is _EQ allowed for both cases?
                    if (!(tokenLowerBound.inequality == Inequality.GREATER_THAN_EQ
                          && tokenUpperBound.inequality == Inequality.LESS_THAN_EQ))
                    {
                        // token < 42 and >= 42... nothing matches that!
                        unmatchable = true;
                        tokenLowerBound = null;
                        tokenUpperBound = null;
                    }
                }
            }
        }

        private void addConditional(Conditional conditional)
        {
            if (conditional instanceof Conditional.Where)
            {
                Conditional.Where w = (Conditional.Where) conditional;
                if (w.kind == Inequality.NOT_EQUAL)
                    throw new UnsupportedOperationException("!= is currently not supported");
                if (w.lhs instanceof Symbol)
                {
                    Symbol col = (Symbol) w.lhs;
                    switch (w.kind)
                    {
                        case EQUAL:
                            var override = eq.put(col, Collections.singletonList(w.rhs));
                            if (override != null)
                                throw new IllegalStateException("Column " + col.detailedName() + " had 2 '=' statements...");
                            break;
                        case LESS_THAN:
                        case LESS_THAN_EQ:
                        case GREATER_THAN:
                        case GREATER_THAN_EQ:
                            ltOrGt.computeIfAbsent(col, i -> new ArrayList<>()).add(new ColumnCondition(w.kind, eval(w.rhs)));
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
                                    token = ref.token;
                                    break;
                                case LESS_THAN:
                                case LESS_THAN_EQ:
                                    if (tokenUpperBound == null)
                                    {
                                        tokenUpperBound = new TokenCondition(w.kind, ref.token);
                                    }
                                    else if (tokenUpperBound.token.equals(ref.token))
                                    {
                                        // 2 cases
                                        // a < ? AND a < ? - nothing to see here
                                        // a < ? AND a <= ? - in this case we need the most restrictive option of <
                                        if (tokenUpperBound.inequality != w.kind)
                                            tokenUpperBound = new TokenCondition(Inequality.LESS_THAN, ref.token);
                                    }
                                    else
                                    {
                                        // given this is < semantic, the smallest token wins
                                        if (ref.token.compareTo(tokenUpperBound.token) < 0)
                                            tokenUpperBound = new TokenCondition(w.kind, ref.token);
                                    }
                                    break;
                                case GREATER_THAN:
                                case GREATER_THAN_EQ:
                                    if (tokenLowerBound == null)
                                    {
                                        tokenLowerBound = new TokenCondition(w.kind, ref.token);
                                    }
                                    else if (tokenLowerBound.token.equals(ref.token))
                                    {
                                        // 2 cases
                                        // a > ? AND a > ? - nothing to see here
                                        // a > ? AND a >= ? - in this case we need the most restrictive option of >
                                        if (tokenLowerBound.inequality != w.kind)
                                            tokenLowerBound = new TokenCondition(Inequality.GREATER_THAN, ref.token);
                                    }
                                    else
                                    {
                                        // given this is > semantic, the latest token wins
                                        if (ref.token.compareTo(tokenLowerBound.token) > 0)
                                            tokenLowerBound = new TokenCondition(w.kind, ref.token);
                                    }
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
                    var override = eq.put(col, in.expressions);
                    if (override != null)
                        throw new IllegalStateException("Column " + col.detailedName() + " had 2 '=' statements...");
                    //TODO (correctness): can't find any documentation saying clustering is ordered by the data... it "could" but is it garanateed?
                    if (factory.partitionColumns.contains(col) || factory.clusteringColumns.contains(col))
                        unordered = true;
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
                    List<ColumnCondition> list = ltOrGt.computeIfAbsent(col, i -> new ArrayList<>());
                    list.add(new ColumnCondition(Inequality.GREATER_THAN_EQ, eval(between.start)));
                    list.add(new ColumnCondition(Inequality.LESS_THAN_EQ, eval(between.end)));
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
                                token = startToken;
                            }
                            else if (startToken.compareTo(endToken) > 0)
                            {
                                // start is larger than end... no matches
                                unmatchable = true;
                            }
                            else
                            {
                                tokenLowerBound = new TokenCondition(Inequality.GREATER_THAN_EQ, startToken);
                                tokenUpperBound = new TokenCondition(Inequality.LESS_THAN_EQ, endToken);
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
                addConditional(and.left);
                addConditional(and.right);
            }
            else
            {
                //TODO (coverage): IS
                throw new UnsupportedOperationException(conditional.getClass().getCanonicalName());
            }
        }

        boolean include(BytesPartitionState partition)
        {
            if (unmatchable) return false;
            // did we include a bad partition?
            if (partition.shouldDelete()) return false;
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
                    if (actual == null)
                        return false;
                    if (!matches(actual, eq.get(col)))
                        return false;
                }
                if (ltOrGt.containsKey(col))
                {
                    ByteBuffer actual = accessor.apply(columns.indexOf(col));
                    if (actual == null)
                        return false;
                    if (!matches(col.type(), actual, ltOrGt.get(col)))
                        return false;
                }
            }
            return true;
        }

        private boolean testsClustering()
        {
            return factory.clusteringColumns.stream().anyMatch(eq::containsKey)
                   || factory.clusteringColumns.stream().anyMatch(ltOrGt::containsKey);
        }

        private boolean testsRegular()
        {
            return factory.regularColumns.stream().anyMatch(eq::containsKey)
                   || factory.regularColumns.stream().anyMatch(ltOrGt::containsKey);
        }

        private boolean testsRow()
        {
            return testsClustering() || testsRegular();
        }
    }

    private static class ColumnCondition
    {
        private final Inequality inequality;
        private final ByteBuffer value;

        private ColumnCondition(Inequality inequality, ByteBuffer value)
        {
            this.inequality = inequality;
            this.value = value;
        }
    }

    private static class TokenCondition
    {
        private final Inequality inequality;
        private final Token token;

        private TokenCondition(Inequality inequality, Token token)
        {
            this.inequality = inequality;
            this.token = token;
        }
    }
}
