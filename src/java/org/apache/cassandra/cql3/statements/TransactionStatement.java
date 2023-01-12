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

package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Txn;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.RowDataReference;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.cql3.transactions.SelectReferenceSource;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnDataName;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LazyToString;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class TransactionStatement implements CQLStatement
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionStatement.class);

    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PRIMARY_KEY_LET_MESSAGE = "SELECT in LET assignment without LIMIT 1 must specify all primary key elements; CQL %s";
    public static final String INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE = "Normal SELECT without LIMIT 1 must specify all primary key elements; CQL %s";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions.";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps.";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";
    public static final String SELECT_REFS_NEED_COLUMN_MESSAGE = "SELECT references must specify a column.";

    static class NamedSelect
    {
        final TxnDataName name;
        final SelectStatement select;

        public NamedSelect(TxnDataName name, SelectStatement select)
        {
            this.name = name;
            this.select = select;
        }
    }

    private final List<NamedSelect> assignments;
    private final NamedSelect returningSelect;
    private final List<RowDataReference> returningReferences;
    private final List<ModificationStatement> updates;
    private final List<ConditionStatement> conditions;

    private final VariableSpecifications bindVariables;

    private final Map<TxnDataName, NamedSelect> autoReads = new HashMap<>();

    public TransactionStatement(List<NamedSelect> assignments,
                                NamedSelect returningSelect,
                                List<RowDataReference> returningReferences,
                                List<ModificationStatement> updates,
                                List<ConditionStatement> conditions,
                                VariableSpecifications bindVariables)
    {
        this.assignments = assignments;
        this.returningSelect = returningSelect;
        this.returningReferences = returningReferences;
        this.updates = updates;
        this.conditions = conditions;
        this.bindVariables = bindVariables;
    }

    public List<ModificationStatement> getUpdates()
    {
        return updates;
    }

    @Override
    public List<ColumnSpecification> getBindVariables()
    {
        return bindVariables.getBindVariables();
    }

    @Override
    public void authorize(ClientState state)
    {
        // Assess read permissions for all data from both explicit LET statements and generated reads.
        for (NamedSelect let : assignments)
            let.select.authorize(state);

        if (returningSelect != null)
            returningSelect.select.authorize(state);

        for (ModificationStatement update : updates)
            update.authorize(state);
    }

    @Override
    public void validate(ClientState state)
    {
        for (NamedSelect statement : assignments)
            statement.select.validate(state);
        if (returningSelect != null)
            returningSelect.select.validate(state);
        for (ModificationStatement statement : updates)
            statement.validate(state);
    }

    TxnNamedRead createNamedRead(NamedSelect namedSelect, QueryOptions options)
    {
        SelectStatement select = namedSelect.select;
        ReadQuery readQuery = select.getQuery(options, 0);

        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;

        if (selectQuery.queries.size() != 1)
            throw new IllegalArgumentException("Within a transaction, SELECT statements must select a single partition; found " + selectQuery.queries.size() + " partitions");

        return new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries));
    }

    List<TxnNamedRead> createNamedReads(NamedSelect namedSelect, QueryOptions options)
    {
        SelectStatement select = namedSelect.select;
        ReadQuery readQuery = select.getQuery(options, 0);

        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) readQuery;

        if (selectQuery.queries.size() == 1)
            return Collections.singletonList(new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries)));

        List<TxnNamedRead> list = new ArrayList<>(selectQuery.queries.size());
        for (int i = 0; i < selectQuery.queries.size(); i++)
            list.add(new TxnNamedRead(TxnDataName.returning(i), selectQuery.queries.get(i)));
        return list;
    }

    private List<TxnNamedRead> createNamedReads(QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnNamedRead> reads = new ArrayList<>(assignments.size() + 1);

        for (NamedSelect select : assignments)
        {
            TxnNamedRead read = createNamedRead(select, options);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        if (returningSelect != null)
        {
            for (TxnNamedRead read : createNamedReads(returningSelect, options))
            {
                keyConsumer.accept(read.key());
                reads.add(read);
            }
        }

        for (NamedSelect select : autoReads.values())
            // don't need keyConsumer as the keys are known to exist due to Modification
            reads.add(createNamedRead(select, options));

        return reads;
    }

    TxnCondition createCondition(QueryOptions options)
    {
        if (conditions.isEmpty())
            return TxnCondition.none();
        if (conditions.size() == 1)
            return conditions.get(0).createCondition(options);

        List<TxnCondition> result = new ArrayList<>(conditions.size());
        for (ConditionStatement condition : conditions)
            result.add(condition.createCondition(options));

        // TODO: OR support
        return new TxnCondition.BooleanGroup(TxnCondition.Kind.AND, result);
    }

    List<TxnWrite.Fragment> createWriteFragments(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        List<TxnWrite.Fragment> fragments = new ArrayList<>(updates.size());
        int idx = 0;
        for (ModificationStatement modification : updates)
        {
            TxnWrite.Fragment fragment = modification.getTxnWriteFragment(idx, state, options);
            keyConsumer.accept(fragment.key);
            fragments.add(fragment);

            if (modification.allReferenceOperations().stream().anyMatch(ReferenceOperation::requiresRead))
            {
                // Reads are not merged by partition here due to potentially differing columns retrieved, etc.
                TxnDataName partitionName = TxnDataName.partitionRead(modification.metadata(), fragment.key.partitionKey(), idx);
                if (!autoReads.containsKey(partitionName))
                    autoReads.put(partitionName, new NamedSelect(partitionName, modification.createSelectForTxn()));
            }

            idx++;
        }
        return fragments;
    }

    TxnUpdate createUpdate(ClientState state, QueryOptions options, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(state, options, keyConsumer), createCondition(options));
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    @VisibleForTesting
    public Txn createTxn(ClientState state, QueryOptions options)
    {
        SortedSet<Key> keySet = new TreeSet<>();

        if (updates.isEmpty())
        {
            // TODO: Test case around this...
            Preconditions.checkState(conditions.isEmpty(), "No condition should exist without updates present");
            List<TxnNamedRead> reads = createNamedReads(options, keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(reads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL);
        }
        else
        {
            TxnUpdate update = createUpdate(state, options, keySet::add);
            List<TxnNamedRead> reads = createNamedReads(options, keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnRead read = new TxnRead(reads, txnKeys);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL, update);
        }
    }

    private static void checkAtMostOneRowSpecified(ClientState clientState, @Nullable QueryOptions options, SelectStatement select, String failureMessage)
    {
        if (select.getRestrictions().hasAllPKColumnsRestrictedByEqualities())
            return;

        if (options == null)
        {
            // If the limit is a non-terminal marker (because we're preparing), defer validation until execution.
            if (select.isLimitMarker())
                return;

            // The limit is already defined, so proceed with validation...
            options = QueryOptions.DEFAULT;
        }

        int limit = select.getLimit(options);
        QueryOptions finalOptions = options; // javac thinks this is mutable so requires a copy
        checkTrue(limit == 1, failureMessage, LazyToString.lazy(() -> select.asCQL(finalOptions, clientState)));
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    {
        try
        {
            for (NamedSelect assignment : assignments)
                checkAtMostOneRowSpecified(state.getClientState(), options, assignment.select, INCOMPLETE_PRIMARY_KEY_LET_MESSAGE);

            if (returningSelect != null)
                checkAtMostOneRowSpecified(state.getClientState(), options, returningSelect.select, INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE);

            TxnData data = AccordService.instance().coordinate(createTxn(state.getClientState(), options), options);

            if (returningSelect != null)
            {
                SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) returningSelect.select.getQuery(options, 0);
                Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
                ResultSetBuilder result = new ResultSetBuilder(returningSelect.select.getResultMetadata(), selectors, null);
                if (selectQuery.queries.size() == 1)
                {
                    FilteredPartition partition = data.get(TxnDataName.returning());
                    returningSelect.select.processPartition(partition.rowIterator(), options, result, FBUtilities.nowInSeconds());
                }
                else
                {
                    int nowInSec = FBUtilities.nowInSeconds();
                    for (int i = 0; i < selectQuery.queries.size(); i++)
                    {
                        FilteredPartition partition = data.get(TxnDataName.returning(i));
                        returningSelect.select.processPartition(partition.rowIterator(), options, result, nowInSec);
                    }
                }
                return new ResultMessage.Rows(result.build());
            }

            if (returningReferences != null)
            {
                List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
                List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());

                for (RowDataReference reference : returningReferences)
                {
                    ColumnMetadata forMetadata = reference.toResultMetadata();
                    names.add(forMetadata);
                    columns.add(reference.column());
                }

                ResultSetBuilder result = new ResultSetBuilder(new ResultSet.ResultMetadata(names), Selection.noopSelector(), null);
                result.newRow(options.getProtocolVersion(), null, null, columns);

                for (int i = 0; i < returningReferences.size(); i++)
                {
                    RowDataReference reference = returningReferences.get(i);
                    TxnReference txnReference = reference.toTxnReference(options);
                    ByteBuffer buffer = txnReference.toByteBuffer(data, names.get(i).type);
                    result.add(buffer);
                }

                return new ResultMessage.Rows(result.build());
            }

            // In the case of a write-only transaction, just return and empty result.
            // TODO: This could be modified to return an indication of whether a condition (if present) succeeds.
            return new ResultMessage.Void();
        }
        catch (Throwable t)
        {
           logger.error("Unexpected error with transaction", t);
           throw t;
        }
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, options, nanoTime());
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRANSACTION);
    }

    public static class Parsed extends QualifiedStatement
    {
        private final List<SelectStatement.RawStatement> assignments;
        private final SelectStatement.RawStatement select;
        private final List<RowDataReference.Raw> returning;
        private final List<ModificationStatement.Parsed> updates;
        private final List<ConditionStatement.Raw> conditions;
        private final List<RowDataReference.Raw> dataReferences;

        public Parsed(List<SelectStatement.RawStatement> assignments,
                      SelectStatement.RawStatement select,
                      List<RowDataReference.Raw> returning,
                      List<ModificationStatement.Parsed> updates,
                      List<ConditionStatement.Raw> conditions,
                      List<RowDataReference.Raw> dataReferences)
        {
            super(null);
            this.assignments = assignments;
            this.select = select;
            this.returning = returning;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.dataReferences = dataReferences;
        }

        @Override
        public void setKeyspace(ClientState state)
        {
            assignments.forEach(select -> select.setKeyspace(state));
            if (select != null)
                select.setKeyspace(state);
            updates.forEach(update -> update.setKeyspace(state));
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkFalse(updates.isEmpty() && returning == null && select == null, EMPTY_TRANSACTION_MESSAGE);

            if (select != null || returning != null)
                checkTrue(select != null ^ returning != null, "Cannot specify both a full SELECT and a SELECT w/ LET references.");

            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<TxnDataName, RowDataReference.ReferenceSource> refSources = new HashMap<>();
            Set<TxnDataName> selectNames = new HashSet<>();

            for (SelectStatement.RawStatement select : assignments)
            {
                checkNotNull(select.parameters.refName, "Assignments must be named");
                TxnDataName name = TxnDataName.user(select.parameters.refName);
                checkTrue(selectNames.add(name), DUPLICATE_TUPLE_NAME_MESSAGE, name.name());

                SelectStatement prepared = select.prepare(bindVariables);
                NamedSelect namedSelect = new NamedSelect(name, prepared);
                checkAtMostOneRowSpecified(state, null, namedSelect.select, INCOMPLETE_PRIMARY_KEY_LET_MESSAGE);
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(prepared));
            }

            if (dataReferences != null)
                for (RowDataReference.Raw reference : dataReferences)
                    reference.resolveReference(refSources);

            NamedSelect returningSelect = null;
            if (select != null)
            {
                returningSelect = new NamedSelect(TxnDataName.returning(), select.prepare(bindVariables));
                checkAtMostOneRowSpecified(state, null, returningSelect.select, INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE);
            }

            List<RowDataReference> returningReferences = null;

            if (returning != null)
            {
                // TODO: Eliminate/modify this check if we allow full tuple selections.
                returningReferences = returning.stream().peek(raw -> checkTrue(raw.column() != null, SELECT_REFS_NEED_COLUMN_MESSAGE))
                                                        .map(RowDataReference.Raw::prepareAsReceiver)
                                                        .collect(Collectors.toList());
            }

            List<ModificationStatement> preparedUpdates = new ArrayList<>(updates.size());
            
            // check for any read-before-write updates
            for (int i = 0; i < updates.size(); i++)
            {
                ModificationStatement.Parsed parsed = updates.get(i);

                ModificationStatement prepared = parsed.prepare(bindVariables);
                checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE);
                checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE);

                preparedUpdates.add(prepared);
            }

            List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
            for (ConditionStatement.Raw condition : conditions)
                // TODO: If we eventually support IF ks.function(ref) THEN, the keyspace will have to be provided here
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            return new TransactionStatement(preparedAssignments, returningSelect, returningReferences, preparedUpdates, preparedConditions, bindVariables);
        }
    }
}
