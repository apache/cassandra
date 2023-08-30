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

import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.selection.RawSelector;
import org.apache.cassandra.cql3.selection.Selectable;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.ViewMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A View copies data from a base table into a view table which can be queried independently from the
 * base. Every update which targets the base table must be fed through the {@link ViewManager} to ensure
 * that if a view needs to be updated, the updates are properly created and fed into the view.
 */
public class View
{
    public final static String USAGE_WARNING = "Materialized views are experimental and are not recommended for production use.";

    private static final Logger logger = LoggerFactory.getLogger(View.class);

    public final String name;
    private volatile ViewMetadata definition;

    private final ColumnFamilyStore baseCfs;

    public volatile List<ColumnMetadata> baseNonPKColumnsInViewPK;
    private ViewBuilder builder;

    private SelectStatement select;
    private ReadQuery query;

    public View(ViewMetadata definition, ColumnFamilyStore baseCfs)
    {
        this.baseCfs = baseCfs;
        this.name = definition.name();

        updateDefinition(definition);
    }

    public ViewMetadata getDefinition()
    {
        return definition;
    }

    /**
     * This updates the columns stored which are dependent on the base TableMetadata.
     */
    public void updateDefinition(ViewMetadata definition)
    {
        this.definition = definition;
        List<ColumnMetadata> nonPKDefPartOfViewPK = new ArrayList<>();
        for (ColumnMetadata baseColumn : baseCfs.metadata.get().columns())
        {
            ColumnMetadata viewColumn = getViewColumn(baseColumn);
            if (viewColumn != null && !baseColumn.isPrimaryKeyColumn() && viewColumn.isPrimaryKeyColumn())
                nonPKDefPartOfViewPK.add(baseColumn);
        }
        this.baseNonPKColumnsInViewPK = nonPKDefPartOfViewPK;
    }

    /**
     * The view column corresponding to the provided base column. This <b>can</b>
     * return {@code null} if the column is denormalized in the view.
     */
    public ColumnMetadata getViewColumn(ColumnMetadata baseColumn)
    {
        return definition.metadata.getColumn(baseColumn.name);
    }

    /**
     * The base column corresponding to the provided view column. This should
     * never return {@code null} since a view can't have its "own" columns.
     */
    public ColumnMetadata getBaseColumn(ColumnMetadata viewColumn)
    {
        ColumnMetadata baseColumn = baseCfs.metadata().getColumn(viewColumn.name);
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
    public boolean matchesViewFilter(DecoratedKey partitionKey, Row baseRow, long nowInSec)
    {
        return getReadQuery().selectsClustering(partitionKey, baseRow.clustering())
            && getSelectStatement().rowFilterForInternalCalls().isSatisfiedBy(baseCfs.metadata(), partitionKey, baseRow, nowInSec);
    }

    /**
     * Returns the SelectStatement used to populate and filter this view.  Internal users should access the select
     * statement this way to ensure it has been prepared.
     */
    SelectStatement getSelectStatement()
    {
        if (null == select)
        {
            SelectStatement.Parameters parameters =
                new SelectStatement.Parameters(Collections.emptyList(),
                                               Collections.emptyList(),
                                               false,
                                               true,
                                               false);

            SelectStatement.RawStatement rawSelect =
                new SelectStatement.RawStatement(new QualifiedName(baseCfs.getKeyspaceName(), baseCfs.name),
                                                 parameters,
                                                 selectClause(),
                                                 definition.whereClause,
                                                 null,
                                                 null);

            rawSelect.setBindVariables(Collections.emptyList());

            select = rawSelect.prepare(ClientState.forInternalCalls(), true);
        }

        return select;
    }

    private List<RawSelector> selectClause()
    {
        return definition.metadata
                         .columns()
                         .stream()
                         .map(c -> c.name.toString())
                         .map(Selectable.RawIdentifier::forQuoted)
                         .map(c -> new RawSelector(c, null))
                         .collect(Collectors.toList());
    }

    /**
     * Returns the ReadQuery used to filter this view.  Internal users should access the query this way to ensure it
     * has been prepared.
     */
    ReadQuery getReadQuery()
    {
        if (query == null)
            query = getSelectStatement().getQuery(QueryOptions.forInternalCalls(Collections.emptyList()), FBUtilities.nowInSeconds());

        return query;
    }

    public synchronized void build()
    {
        stopBuild();
        builder = new ViewBuilder(baseCfs, this);
        builder.start();
    }

    /**
     * Stops the building of this view, no-op if it isn't building.
     */
    synchronized void stopBuild()
    {
        if (builder != null)
        {
            logger.debug("Stopping current view builder due to schema change or truncate");
            builder.stop();
            builder = null;
        }
    }

    @Nullable
    public static TableMetadataRef findBaseTable(String keyspace, String viewName)
    {
        ViewMetadata view = Schema.instance.getView(keyspace, viewName);
        return (view == null) ? null : Schema.instance.getTableMetadataRef(view.baseTableId);
    }

    // TODO: REMOVE
    public static Iterable<ViewMetadata> findAll(String keyspace, String baseTable)
    {
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        return Iterables.filter(ksm.views, view -> view.baseTableName.equals(baseTable));
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
