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
package org.apache.cassandra.db.view;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.config.ViewDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.MultiColumnRelation;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.Relation;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A View copies data from a base table into a view table which can be queried independently from the
 * base. Every update which targets the base table must be fed through the {@link ViewManager} to ensure
 * that if a view needs to be updated, the updates are properly created and fed into the view.
 */
public class View
{
    private static final Logger logger = LoggerFactory.getLogger(View.class);

    public final String name;
    private volatile ViewDefinition definition;

    private final ColumnFamilyStore baseCfs;

    public volatile List<ColumnDefinition> baseNonPKColumnsInViewPK;

    private ViewBuilder builder;

    // Only the raw statement can be final, because the statement cannot always be prepared when the MV is initialized.
    // For example, during startup, this view will be initialized as part of the Keyspace.open() work; preparing a statement
    // also requires the keyspace to be open, so this results in double-initialization problems.
    private final SelectStatement.RawStatement rawSelect;
    private SelectStatement select;
    private ReadQuery query;

    public View(ViewDefinition definition,
                ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.name = definition.viewName;
        this.rawSelect = definition.select;

        updateDefinition(definition);
    }

    public ViewDefinition getDefinition()
    {
        return definition;
    }

    /**
     * This updates the columns stored which are dependent on the base CFMetaData.
     */
    public void updateDefinition(ViewDefinition definition)
    {
        this.definition = definition;

        List<ColumnDefinition> nonPKDefPartOfViewPK = new ArrayList<>();
        for (ColumnDefinition baseColumn : baseCfs.metadata.allColumns())
        {
            ColumnDefinition viewColumn = getViewColumn(baseColumn);
            if (viewColumn != null && !baseColumn.isPrimaryKeyColumn() && viewColumn.isPrimaryKeyColumn())
                nonPKDefPartOfViewPK.add(baseColumn);
        }
        this.baseNonPKColumnsInViewPK = nonPKDefPartOfViewPK;
    }

    /**
     * The view column corresponding to the provided base column. This <b>can</b>
     * return {@code null} if the column is denormalized in the view.
     */
    public ColumnDefinition getViewColumn(ColumnDefinition baseColumn)
    {
        return definition.metadata.getColumnDefinition(baseColumn.name);
    }

    /**
     * The base column corresponding to the provided view column. This should
     * never return {@code null} since a view can't have its "own" columns.
     */
    public ColumnDefinition getBaseColumn(ColumnDefinition viewColumn)
    {
        ColumnDefinition baseColumn = baseCfs.metadata.getColumnDefinition(viewColumn.name);
        assert baseColumn != null;
        return baseColumn;
    }

    /**
     * Whether the view might be affected by the provided update.
     * <p>
     * Note that having this method return {@code true} is not an absolute guarantee that the view will be
     * updated, just that it most likely will, but a {@code false} return guarantees it won't be affected).
     *
     * @param partitionKey the partition key that is updated.
     * @param update the update being applied.
     * @return {@code false} if we can guarantee that inserting {@code update} for key {@code partitionKey}
     * won't affect the view in any way, {@code true} otherwise.
     */
    public boolean mayBeAffectedBy(DecoratedKey partitionKey, Row update)
    {
        // We can guarantee that the view won't be affected if:
        //  - the clustering is excluded by the view filter (note that this isn't true of the filter on regular columns:
        //    even if an update don't match a view condition on a regular column, that update can still invalidate an pre-existing
        //    entry).
        //  - or the update don't modify any of the columns impacting the view (where "impacting" the view means that column is
        //    neither included in the view, nor used by the view filter).
        if (!getReadQuery().selectsClustering(partitionKey, update.clustering()))
            return false;
        return true;
    }

    /**
     * Whether a given base row matches the view filter (and thus if is should have a corresponding entry).
     * <p>
     * Note that this differs from {@link #mayBeAffectedBy} in that the provide row <b>must</b> be the current
     * state of the base row, not just some updates to it. This method also has no false positive: a base
     * row either do or don't match the view filter.
     *
     * @param partitionKey the partition key that is updated.
     * @param baseRow the current state of a particular base row.
     * @param nowInSec the current time in seconds (to decide what is live and what isn't).
     * @return {@code true} if {@code baseRow} matches the view filters, {@code false} otherwise.
     */
    public boolean matchesViewFilter(DecoratedKey partitionKey, Row baseRow, int nowInSec)
    {
        return getReadQuery().selectsClustering(partitionKey, baseRow.clustering())
            && getSelectStatement().rowFilterForInternalCalls().isSatisfiedBy(baseCfs.metadata, partitionKey, baseRow, nowInSec);
    }

    /**
     * Returns the SelectStatement used to populate and filter this view.  Internal users should access the select
     * statement this way to ensure it has been prepared.
     */
    public SelectStatement getSelectStatement()
    {
        if (select == null)
        {
            ClientState state = ClientState.forInternalCalls();
            state.setKeyspace(baseCfs.keyspace.getName());
            rawSelect.prepareKeyspace(state);
            ParsedStatement.Prepared prepared = rawSelect.prepare(true, ClientState.forInternalCalls());
            select = (SelectStatement) prepared.statement;
        }

        return select;
    }

    /**
     * Returns the ReadQuery used to filter this view.  Internal users should access the query this way to ensure it
     * has been prepared.
     */
    public ReadQuery getReadQuery()
    {
        if (query == null)
        {
            query = getSelectStatement().getQuery(QueryOptions.forInternalCalls(Collections.emptyList()), FBUtilities.nowInSeconds());
            logger.trace("View query: {}", rawSelect);
        }

        return query;
    }

    public synchronized void build()
    {
        if (this.builder != null)
        {
            logger.debug("Stopping current view builder due to schema change");
            this.builder.stop();
            this.builder = null;
        }

        this.builder = new ViewBuilder(baseCfs, this);
        CompactionManager.instance.submitViewBuilder(builder);
    }

    @Nullable
    public static CFMetaData findBaseTable(String keyspace, String viewName)
    {
        ViewDefinition view = Schema.instance.getView(keyspace, viewName);
        return (view == null) ? null : Schema.instance.getCFMetaData(view.baseTableId);
    }

    public static Iterable<ViewDefinition> findAll(String keyspace, String baseTable)
    {
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
        final UUID baseId = Schema.instance.getId(keyspace, baseTable);
        return Iterables.filter(ksm.views, view -> view.baseTableId.equals(baseId));
    }

    /**
     * Builds the string text for a materialized view's SELECT statement.
     */
    public static String buildSelectStatement(String cfName, Collection<ColumnDefinition> includedColumns, String whereClause)
    {
         StringBuilder rawSelect = new StringBuilder("SELECT ");
        if (includedColumns == null || includedColumns.isEmpty())
            rawSelect.append("*");
        else
            rawSelect.append(includedColumns.stream().map(id -> id.name.toCQLString()).collect(Collectors.joining(", ")));
        rawSelect.append(" FROM \"").append(cfName).append("\" WHERE ") .append(whereClause).append(" ALLOW FILTERING");
        return rawSelect.toString();
    }

    public static String relationsToWhereClause(List<Relation> whereClause)
    {
        List<String> expressions = new ArrayList<>(whereClause.size());
        for (Relation rel : whereClause)
        {
            StringBuilder sb = new StringBuilder();

            if (rel.isMultiColumn())
            {
                sb.append(((MultiColumnRelation) rel).getEntities().stream()
                        .map(ColumnDefinition.Raw::toString)
                        .collect(Collectors.joining(", ", "(", ")")));
            }
            else
            {
                sb.append(((SingleColumnRelation) rel).getEntity());
            }

            sb.append(" ").append(rel.operator()).append(" ");

            if (rel.isIN())
            {
                sb.append(rel.getInValues().stream()
                        .map(Term.Raw::getText)
                        .collect(Collectors.joining(", ", "(", ")")));
            }
            else
            {
                sb.append(rel.getValue().getText());
            }

            expressions.add(sb.toString());
        }

        return expressions.stream().collect(Collectors.joining(" AND "));
    }

    public boolean hasSamePrimaryKeyColumnsAsBaseTable()
    {
        return baseNonPKColumnsInViewPK.isEmpty();
    }

    /**
     * When views contains a primary key column that is not part
     * of the base table primary key, we use that column liveness
     * info as the view PK, to ensure that whenever that column
     * is not live in the base, the row is not live in the view.
     *
     * This is done to prevent cells other than the view PK from
     * making the view row alive when the view PK column is not
     * live in the base. So in this case we tie the row liveness,
     * to the primary key liveness.
     *
     * See CASSANDRA-11500 for context.
     */
    public boolean enforceStrictLiveness()
    {
        return !baseNonPKColumnsInViewPK.isEmpty();
    }
}
