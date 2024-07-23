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
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.api.Key;
import accord.primitives.Keys;
import accord.primitives.Txn;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.selection.ResultSetBuilder;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.transactions.ConditionStatement;
import org.apache.cassandra.cql3.transactions.ReferenceOperation;
import org.apache.cassandra.cql3.transactions.RowDataReference;
import org.apache.cassandra.cql3.transactions.SelectReferenceSource;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadQuery;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnCondition;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnDataKeyValue;
import org.apache.cassandra.service.accord.txn.TxnKeyRead;
import org.apache.cassandra.service.accord.txn.TxnNamedRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnReference;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.service.accord.txn.TxnUpdate;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Read;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkNotNull;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.AUTO_READ;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.RETURNING;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.USER;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.apache.cassandra.service.accord.txn.TxnKeyRead.createTxnRead;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.retry_new_protocol;

public class TransactionStatement implements CQLStatement.CompositeCQLStatement, CQLStatement.ReturningCQLStatement
{
    public static final String DUPLICATE_TUPLE_NAME_MESSAGE = "The name '%s' has already been used by a LET assignment.";
    public static final String INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE = "SELECT must specify either all partition key elements. Partition key elements must be always specified with equality operators; %s %s";
    public static final String INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE = "SELECT must specify either all primary key elements or all partition key elements and LIMIT 1. In both cases partition key elements must be always specified with equality operators; %s %s";
    public static final String NO_CONDITIONS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify their own conditions; %s statement %s";
    public static final String NO_TIMESTAMPS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom timestamps; %s statement %s";
    public static final String NO_TTLS_IN_UPDATES_MESSAGE = "Updates within transactions may not specify custom ttls; %s statement %s";
    public static final String TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE = "Accord transactions are disabled on table (See transactional_mode in table options); %s statement %s";
    public static final String TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE = "Accord transactions are disabled on table (table is being dropped); %s statement %s";
    public static final String NO_COUNTERS_IN_TXNS_MESSAGE = "Counter columns cannot be accessed within a transaction; %s statement %s";
    public static final String NO_AGGREGATION_IN_TXNS_MESSAGE = "No aggregation functions allowed within a transaction; %s statement %s";
    public static final String NO_ORDER_BY_IN_TXNS_MESSAGE = "No ORDER BY clause allowed within a transaction; %s statement %s";
    public static final String NO_GROUP_BY_IN_TXNS_MESSAGE = "No GROUP BY clause allowed within a transaction; %s statement %s";
    public static final String EMPTY_TRANSACTION_MESSAGE = "Transaction contains no reads or writes";
    public static final String SELECT_REFS_NEED_COLUMN_MESSAGE = "SELECT references must specify a column.";
    public static final String TRANSACTIONS_DISABLED_MESSAGE = "Accord transactions are disabled. (See accord.enabled in cassandra.yaml)";
    public static final String ILLEGAL_RANGE_QUERY_MESSAGE = "Range queries are not allowed for reads within a transaction; %s %s";
    public static final String UNSUPPORTED_MIGRATION = "Transaction Statement is unsupported when migrating away from Accord or before migration to Accord is complete for a range";
    public static final String NO_PARTITION_IN_CLAUSE_WITH_LIMIT = "Partition key is present in IN clause and there is a LIMIT... this is currently not supported; %s statement %s";

    static class NamedSelect
    {
        final int name;
        final SelectStatement select;

        public NamedSelect(int name, SelectStatement select)
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
    private final ResultSet.ResultMetadata resultMetadata;

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

        if (returningSelect != null)
        {
            resultMetadata = returningSelect.select.getResultMetadata();
        }
        else if (returningReferences != null && !returningReferences.isEmpty())
        {
            List<ColumnSpecification> names = new ArrayList<>(returningReferences.size());
            for (RowDataReference reference : returningReferences)
                names.add(reference.toResultMetadata());
            resultMetadata = new ResultSet.ResultMetadata(names);
        }
        else
        {
            resultMetadata =  ResultSet.ResultMetadata.EMPTY;
        }
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

    @Override
    public Iterable<CQLStatement> getStatements()
    {
        return () -> {
            Stream<CQLStatement> stream = assignments.stream().map(n -> n.select);
            if (returningSelect != null)
                stream = Stream.concat(stream, Stream.of(returningSelect.select));
            stream = Stream.concat(stream, updates.stream());
            return stream.iterator();
        };
    }

    @Override
    public ResultSet.ResultMetadata getResultMetadata()
    {
        return resultMetadata;
    }

    TxnNamedRead createNamedRead(NamedSelect namedSelect, QueryOptions options, ClientState state)
    {
        SelectStatement select = namedSelect.select;
        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) select.getQuery(options, 0);

        if (selectQuery.queries.size() != 1)
            throw new IllegalArgumentException("Within a transaction, SELECT statements must select a single partition; found " + selectQuery.queries.size() + " partitions");

        return new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries));
    }

    List<TxnNamedRead> createNamedReads(NamedSelect namedSelect, QueryOptions options, ClientState state)
    {
        SelectStatement select = namedSelect.select;
        // We reject reads from both LET and SELECT that do not specify a single row.
        @SuppressWarnings("unchecked")
        SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) select.getQuery(options, 0);

        if (selectQuery.queries.size() == 1)
            return Collections.singletonList(new TxnNamedRead(namedSelect.name, Iterables.getOnlyElement(selectQuery.queries)));

        List<TxnNamedRead> list = new ArrayList<>(selectQuery.queries.size());
        for (int i = 0; i < selectQuery.queries.size(); i++)
            list.add(new TxnNamedRead(txnDataName(RETURNING, i), selectQuery.queries.get(i)));
        return list;
    }

    private List<TxnNamedRead> createNamedReads(QueryOptions options, ClientState state, @Nullable Int2ObjectHashMap<NamedSelect> autoReads, Consumer<Key> keyConsumer)
    {
        List<TxnNamedRead> reads = new ArrayList<>(assignments.size() + 1);

        for (NamedSelect select : assignments)
        {
            TxnNamedRead read = createNamedRead(select, options, state);
            keyConsumer.accept(read.key());
            reads.add(read);
        }

        if (returningSelect != null)
        {
            for (TxnNamedRead read : createNamedReads(returningSelect, options, state))
            {
                keyConsumer.accept(read.key());
                reads.add(read);
            }
        }

        if (autoReads != null)
        {
            for (NamedSelect select : autoReads.values())
                // don't need keyConsumer as the keys are known to exist due to Modification
                reads.add(createNamedRead(select, options, state));
        }

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

    List<TxnWrite.Fragment> createWriteFragments(ClientState state, QueryOptions options, Map<Integer, NamedSelect> autoReads, Consumer<Key> keyConsumer)
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
                int partitionName = txnDataName(AUTO_READ, idx);
                if (!autoReads.containsKey(partitionName))
                    autoReads.put(partitionName, new NamedSelect(partitionName, modification.createSelectForTxn()));
            }

            idx++;
        }
        return fragments;
    }

    AccordUpdate createUpdate(ClientState state, QueryOptions options, Map<Integer, NamedSelect> autoReads, Consumer<Key> keyConsumer)
    {
        return new TxnUpdate(createWriteFragments(state, options, autoReads, keyConsumer), createCondition(options), null, false);
    }

    Keys toKeys(SortedSet<Key> keySet)
    {
        return new Keys(keySet);
    }

    private static TransactionalMode transactionalModeForSingleKey(Keys keys)
    {
        return Schema.instance.getTableMetadata(((AccordRoutableKey) keys.get(0)).table()).params.transactionalMode;
    }

    @VisibleForTesting
    public Txn createTxn(ClientState state, QueryOptions options)
    {
        SortedSet<Key> keySet = new TreeSet<>();

        if (updates.isEmpty())
        {
            // TODO: Test case around this...
            Preconditions.checkState(conditions.isEmpty(), "No condition should exist without updates present");
            List<TxnNamedRead> reads = createNamedReads(options, state, null, keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnKeyRead read = createTxnRead(reads, null);
            Txn.Kind kind = txnKeys.size() == 1
                    && transactionalModeForSingleKey(txnKeys) == TransactionalMode.full
                    && DatabaseDescriptor.getAccordEphemeralReadEnabledEnabled()
                    ? EphemeralRead : Read;
            return new Txn.InMemory(kind, txnKeys, read, TxnQuery.ALL, null);
        }
        else
        {
            Int2ObjectHashMap<NamedSelect> autoReads = new Int2ObjectHashMap<>();
            AccordUpdate update = createUpdate(state, options, autoReads, keySet::add);
            List<TxnNamedRead> reads = createNamedReads(options, state, autoReads, keySet::add);
            Keys txnKeys = toKeys(keySet);
            TxnKeyRead read = createTxnRead(reads, null);
            return new Txn.InMemory(txnKeys, read, TxnQuery.ALL, update);
        }
    }

    /**
     * Returns {@code true} only if the statement selects multiple clusterings in a partition
     */
    private static boolean isSelectingMultipleClusterings(SelectStatement select, @Nullable QueryOptions options)
    {
        if (select.getRestrictions().hasAllPrimaryKeyColumnsRestrictedByEqualities())
            return false;

        if (options == null)
        {
            // if the limit is a non-terminal marker (because we're preparing), defer validation until execution (when options != null)
            if (select.isLimitMarker())
                return false;

            options = QueryOptions.DEFAULT;
        }

        return select.getLimit(options) != 1;
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    {
        checkTrue(DatabaseDescriptor.getAccordTransactionsEnabled(), TRANSACTIONS_DISABLED_MESSAGE);

        // check again since now we have query options; note that statements are quaranted to be single partition reads at this point
        for (NamedSelect assignment : assignments)
            checkFalse(isSelectingMultipleClusterings(assignment.select, options), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, "LET assignment", assignment.select.source);

        Txn txn = createTxn(state.getClientState(), options);

        TxnResult txnResult = AccordService.instance().coordinate(txn, options.getConsistency(), requestTime);
        if (txnResult.kind() == retry_new_protocol)
            throw new InvalidRequestException(UNSUPPORTED_MIGRATION);
        TxnData data = (TxnData)txnResult;

        if (returningSelect != null)
        {
            @SuppressWarnings("unchecked")
            SinglePartitionReadQuery.Group<SinglePartitionReadCommand> selectQuery = (SinglePartitionReadQuery.Group<SinglePartitionReadCommand>) returningSelect.select.getQuery(options, 0);
            Selection.Selectors selectors = returningSelect.select.getSelection().newSelectors(options);
            ResultSetBuilder result = new ResultSetBuilder(resultMetadata, selectors, false);
            if (selectQuery.queries.size() == 1)
            {
                TxnDataKeyValue partition = (TxnDataKeyValue)data.get(txnDataName(RETURNING));
                boolean reversed = selectQuery.queries.get(0).isReversed();
                if (partition != null)
                    returningSelect.select.processPartition(partition.rowIterator(reversed), options, result, FBUtilities.nowInSeconds());
            }
            else
            {
                long nowInSec = FBUtilities.nowInSeconds();
                for (int i = 0; i < selectQuery.queries.size(); i++)
                {
                    TxnDataKeyValue partition = (TxnDataKeyValue)data.get(txnDataName(RETURNING, i));
                    boolean reversed = selectQuery.queries.get(i).isReversed();
                    if (partition != null)
                        returningSelect.select.processPartition(partition.rowIterator(reversed), options, result, nowInSec);
                }
            }
            return new ResultMessage.Rows(result.build());
        }

        if (returningReferences != null)
        {
            List<AbstractType<?>> resultType = new ArrayList<>(returningReferences.size());
            List<ColumnMetadata> columns = new ArrayList<>(returningReferences.size());

            for (RowDataReference reference : returningReferences)
            {
                ColumnMetadata forMetadata = reference.toResultMetadata();
                resultType.add(forMetadata.type);
                columns.add(reference.column());
            }

            ResultSetBuilder result = new ResultSetBuilder(resultMetadata, Selection.noopSelector(), false);
            result.newRow(options.getProtocolVersion(), null, null, columns);

            for (int i = 0; i < returningReferences.size(); i++)
            {
                RowDataReference reference = returningReferences.get(i);
                TxnReference txnReference = reference.toTxnReference(options);
                ByteBuffer buffer = txnReference.toByteBuffer(data, resultType.get(i));
                result.add(buffer);
            }

            return new ResultMessage.Rows(result.build());
        }

        // In the case of a write-only transaction, just return and empty result.
        // TODO: This could be modified to return an indication of whether a condition (if present) succeeds.
        return new ResultMessage.Void();
    }

    @Override
    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        return execute(state, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRANSACTION);
    }

    @Override
    public boolean eligibleAsPreparedStatement()
    {
        // false is the default, but still best to be explicit.
        return false;
    }

    private static void validate(SelectStatement.RawStatement select)
    {
        if (select.parameters.orderings != null && !select.parameters.orderings.isEmpty())
            throw invalidRequest(NO_ORDER_BY_IN_TXNS_MESSAGE, "SELECT", select.source);
        if (select.parameters.groups != null && !select.parameters.groups.isEmpty())
            throw invalidRequest(NO_GROUP_BY_IN_TXNS_MESSAGE, "SELECT", select.source);
    }

    private static void validate(SelectStatement prepared)
    {
        if (!prepared.table.isAccordEnabled())
            throw invalidRequest(TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, "SELECT", prepared.source);
        if (prepared.table.params.pendingDrop)
            throw invalidRequest(TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE, "SELECT", prepared.source);
        if (prepared.table.isCounter())
            throw invalidRequest(NO_COUNTERS_IN_TXNS_MESSAGE, "SELECT", prepared.source);
        if (prepared.hasAggregation())
            throw invalidRequest(NO_AGGREGATION_IN_TXNS_MESSAGE, "SELECT", prepared.source);

        if (prepared.getRestrictions().keyIsInRelation())
            checkTrue(prepared.getLimit(null) == DataLimits.NO_LIMIT, NO_PARTITION_IN_CLAUSE_WITH_LIMIT, "SELECT", prepared.source);
    }

    public static class Parsed extends QualifiedStatement.Composite
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
            this.assignments = assignments;
            this.select = select;
            this.returning = returning;
            this.updates = updates;
            this.conditions = conditions != null ? conditions : Collections.emptyList();
            this.dataReferences = dataReferences;
        }

        @Override
        protected Iterable<? extends QualifiedStatement> getStatements()
        {
            Iterable<QualifiedStatement> group = Iterables.concat(assignments, updates);
            if (select != null)
                group = Iterables.concat(group, Collections.singleton(select));
            return group;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            checkFalse(updates.isEmpty() && returning == null && select == null, EMPTY_TRANSACTION_MESSAGE);

            if (select != null || returning != null)
                checkTrue(select != null ^ returning != null, "Cannot specify both a full SELECT and a SELECT w/ LET references.");

            List<NamedSelect> preparedAssignments = new ArrayList<>(assignments.size());
            Map<Integer, RowDataReference.ReferenceSource> refSources = new HashMap<>();
            Set<String> selectNames = new HashSet<>();

            int userReadIndex = 0;
            Map<String, Integer> nameToTxnDataName = new HashMap<>();
            for (SelectStatement.RawStatement select : assignments)
            {
                checkNotNull(select.parameters.refName, "Assignments must be named");
                int name = txnDataName(USER, userReadIndex++);
                nameToTxnDataName.put(select.parameters.refName, name);
                checkTrue(selectNames.add(select.parameters.refName), DUPLICATE_TUPLE_NAME_MESSAGE, select.parameters.refName);
                validate(select);

                SelectStatement prepared = select.prepare(bindVariables);
                validate(prepared);

                NamedSelect namedSelect = new NamedSelect(name, prepared);
                checkAtMostOneRowSpecified(namedSelect.select, "LET assignment " + select.parameters.refName);
                preparedAssignments.add(namedSelect);
                refSources.put(name, new SelectReferenceSource(prepared));
            }

            if (dataReferences != null)
                for (RowDataReference.Raw reference : dataReferences)
                    reference.resolveReference(refSources, nameToTxnDataName, userReadIndex++);

            NamedSelect returningSelect = null;
            if (select != null)
            {
                validate(select);
                SelectStatement prepared = select.prepare(bindVariables);
                validate(prepared);
                returningSelect = new NamedSelect(txnDataName(RETURNING), prepared);
                checkAtMostOnePartitionSpecified(returningSelect.select, "returning select");
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

                ModificationStatement prepared = parsed.prepare(state, bindVariables);
                checkTrue(prepared.metadata().isAccordEnabled(), TRANSACTIONS_DISABLED_ON_TABLE_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.metadata().params.pendingDrop, TRANSACTIONS_DISABLED_ON_TABLE_BEING_DROPPED_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.hasConditions(), NO_CONDITIONS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.isTimestampSet(), NO_TIMESTAMPS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);
                checkFalse(prepared.attrs.isTimeToLiveSet(), NO_TTLS_IN_UPDATES_MESSAGE, prepared.type, prepared.source);

                if (prepared.metadata().isCounter())
                    throw invalidRequest(NO_COUNTERS_IN_TXNS_MESSAGE, prepared.type, prepared.source);

                preparedUpdates.add(prepared);
            }

            List<ConditionStatement> preparedConditions = new ArrayList<>(conditions.size());
            for (ConditionStatement.Raw condition : conditions)
                // TODO: If we eventually support IF ks.function(ref) THEN, the keyspace will have to be provided here
                preparedConditions.add(condition.prepare("[txn]", bindVariables));

            return new TransactionStatement(preparedAssignments, returningSelect, returningReferences, preparedUpdates, preparedConditions, bindVariables);
        }

        /**
         * Do not use this method in execution!!! It is only allowed during prepare because it outputs a query raw text.
         * We don't want it print it for a user who provided an identifier of someone's else prepared statement.
         */
        private static void checkAtMostOnePartitionSpecified(SelectStatement select, String name)
        {
            checkTrue(select.getRestrictions().hasPartitionKeyRestrictions(), INCOMPLETE_PARTITION_KEY_SELECT_MESSAGE, name, select.source);
        }

        /**
         * Do not use this method in execution!!! It is only allowed during prepare because it outputs a query raw text.
         * We don't want it print it for a user who provided an identifier of someone's else prepared statement.
         */
        private static void checkAtMostOneRowSpecified(SelectStatement select, String name)
        {
            checkFalse(select.isPartitionRangeQuery(), ILLEGAL_RANGE_QUERY_MESSAGE, name, select.source);
            checkFalse(isSelectingMultipleClusterings(select, null), INCOMPLETE_PRIMARY_KEY_SELECT_MESSAGE, name, select.source);
        }
    }
}
