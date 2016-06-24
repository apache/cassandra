/*
 *
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
 *
 */
package org.apache.cassandra.index;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.internal.CollatedViewIndexBuilder;
import org.apache.cassandra.index.transactions.IndexTransaction;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Consisting of a top level Index interface and two sub-interfaces which handle read and write operations,
 * Searcher and Indexer respectively, this defines a secondary index implementation.
 * Instantiation is done via reflection and implementations must provide a constructor which takes the base
 * table's ColumnFamilyStore and the IndexMetadata which defines the Index as arguments. e.g:
 *  {@code MyCustomIndex( ColumnFamilyStore baseCfs, IndexMetadata indexDef )}
 *
 * The main interface defines methods for index management, index selection at both write and query time,
 * as well as validation of values that will ultimately be indexed.
 * Two sub-interfaces are also defined, which represent single use helpers for short lived tasks at read and write time.
 * Indexer: an event listener which receives notifications at particular points during an update of a single partition
 *          in the base table.
 * Searcher: performs queries against the index based on a predicate defined in a RowFilter. An instance
 *          is expected to be single use, being involved in the execution of a single ReadCommand.
 *
 * The main interface includes factory methods for obtaining instances of both of the sub-interfaces;
 *
 * The methods defined in the top level interface can be grouped into 3 categories:
 *
 * Management Tasks:
 * This group of methods is primarily concerned with maintenance of secondary indexes are are mainly called from
 * SecondaryIndexManager. It includes methods for registering and un-registering an index, performing maintenance
 * tasks such as (re)building an index from SSTable data, flushing, invalidating and so forth, as well as some to
 * retrieve general metadata about the index (index name, any internal tables used for persistence etc).
 * Several of these maintenance functions have a return type of {@code Callable<?>}; the expectation for these methods is
 * that any work required to be performed by the method be done inside the Callable so that the responsibility for
 * scheduling its execution can rest with SecondaryIndexManager. For instance, a task like reloading index metadata
 * following potential updates caused by modifications to the base table may be performed in a blocking way. In
 * contrast, adding a new index may require it to be built from existing SSTable data, a potentially expensive task
 * which should be performed asynchronously.
 *
 * Index Selection:
 * There are two facets to index selection, write time and read time selection. The former is concerned with
 * identifying whether an index should be informed about a particular write operation. The latter is about providing
 * means to use the index for search during query execution.
 *
 * Validation:
 * Values that may be written to an index are checked as part of input validation, prior to an update or insert
 * operation being accepted.
 *
 *
 * Sub-interfaces:
 *
 * Update processing:
 * Indexes are subscribed to the stream of events generated by modifications to the base table. Subscription is
 * done via first registering the Index with the base table's SecondaryIndexManager. For each partition update, the set
 * of registered indexes are then filtered based on the properties of the update using the selection methods on the main
 * interface described above. Each of the indexes in the filtered set then provides an event listener to receive
 * notifications about the update as it is processed. As such then, a event handler instance is scoped to a single
 * partition update; SecondaryIndexManager obtains a new handler for every update it processes (via a call to the
 * factory method, indexerFor. That handler will then receive all events for the update, before being
 * discarded by the SecondaryIndexManager. Indexer instances are never re-used by SecondaryIndexManager and the
 * expectation is that each call to indexerFor should return a unique instance, or at least if instances can
 * be recycled, that a given instance is only used to process a single partition update at a time.
 *
 * Search:
 * Each query (i.e. a single ReadCommand) that uses indexes will use a single instance of Index.Searcher. As with
 * processing of updates, an Index must be registered with the primary table's SecondaryIndexManager to be able to
 * support queries. During the processing of a ReadCommand, the Expressions in its RowFilter are examined to determine
 * whether any of them are supported by a registered Index. supportsExpression is used to filter out Indexes which
 * cannot support a given Expression. After filtering, the set of candidate indexes are ranked according to the result
 * of getEstimatedResultRows and the most selective (i.e. the one expected to return the smallest number of results) is
 * chosen. A Searcher instance is then obtained from the searcherFor method and used to perform the actual Index lookup.
 * Finally, Indexes can define a post processing step to be performed on the coordinator, after results (partitions from
 * the primary table) have been received from replicas and reconciled. This post processing is defined as a
 * {@code java.util.functions.BiFunction<PartitionIterator, RowFilter, PartitionIterator>}, that is a function which takes as
 * arguments a PartitionIterator (containing the reconciled result rows) and a RowFilter (from the ReadCommand being
 * executed) and returns another iterator of partitions, possibly having transformed the initial results in some way.
 * The post processing function is obtained from the Index's postProcessorFor method; the built-in indexes which ship
 * with Cassandra return a no-op function here.
 *
 * An optional static method may be provided to validate custom index options (two variants are supported):
 *
 * <pre>{@code public static Map<String, String> validateOptions(Map<String, String> options);}</pre>
 *
 * The input is the map of index options supplied in the WITH clause of a CREATE INDEX statement.
 *
 * <pre>{@code public static Map<String, String> validateOptions(Map<String, String> options, CFMetaData cfm);}</pre>
 *
 * In this version, the base table's metadata is also supplied as an argument.
 * If both overloaded methods are provided, only the one including the base table's metadata will be invoked.
 *
 * The validation method should return a map containing any of the supplied options which are not valid for the
 * implementation. If the returned map is not empty, validation is considered failed and an error is raised.
 * Alternatively, the implementation may choose to throw an org.apache.cassandra.exceptions.ConfigurationException
 * if invalid options are encountered.
 *
 */
public interface Index
{

    /*
     * Helpers for building indexes from SSTable data
     */

    /**
     * Provider of {@code SecondaryIndexBuilder} instances. See {@code getBuildTaskSupport} and
     * {@code SecondaryIndexManager.buildIndexesBlocking} for more detail.
     */
    interface IndexBuildingSupport
    {
        SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables);
    }

    /**
     * Default implementation of {@code IndexBuildingSupport} which uses a {@code ReducingKeyIterator} to obtain a
     * collated view of the data in the SSTables.
     */
    public static class CollatedViewIndexBuildingSupport implements IndexBuildingSupport
    {
        public SecondaryIndexBuilder getIndexBuildTask(ColumnFamilyStore cfs, Set<Index> indexes, Collection<SSTableReader> sstables)
        {
            return new CollatedViewIndexBuilder(cfs, indexes, new ReducingKeyIterator(sstables));
        }
    }

    /**
     * Singleton instance of {@code CollatedViewIndexBuildingSupport}, which may be used by any {@code Index}
     * implementation.
     */
    public static final CollatedViewIndexBuildingSupport INDEX_BUILDER_SUPPORT = new CollatedViewIndexBuildingSupport();

    /*
     * Management functions
     */

    /**
     * Get an instance of a helper to provide tasks for building the index from a set of SSTable data.
     * When processing a number of indexes to be rebuilt, {@code SecondaryIndexManager.buildIndexesBlocking} groups
     * those with the same {@code IndexBuildingSupport} instance, allowing multiple indexes to be built with a
     * single pass through the data. The singleton instance returned from the default method implementation builds
     * indexes using a {@code ReducingKeyIterator} to provide a collated view of the SSTable data.
     *
     * @return an instance of the index build taski helper. Index implementations which return <b>the same instance</b>
     * will be built using a single task.
     */
    default IndexBuildingSupport getBuildTaskSupport()
    {
        return INDEX_BUILDER_SUPPORT;
    }

    /**
     * Return a task to perform any initialization work when a new index instance is created.
     * This may involve costly operations such as (re)building the index, and is performed asynchronously
     * by SecondaryIndexManager
     * @return a task to perform any necessary initialization work
     */
    public Callable<?> getInitializationTask();

    /**
     * Returns the IndexMetadata which configures and defines the index instance. This should be the same
     * object passed as the argument to setIndexMetadata.
     * @return the index's metadata
     */
    public IndexMetadata getIndexMetadata();

    /**
     * Return a task to reload the internal metadata of an index.
     * Called when the base table metadata is modified or when the configuration of the Index is updated
     * Implementations should return a task which performs any necessary work to be done due to
     * updating the configuration(s) such as (re)building etc. This task is performed asynchronously
     * by SecondaryIndexManager
     * @return task to be executed by the index manager during a reload
     */
    public Callable<?> getMetadataReloadTask(IndexMetadata indexMetadata);

    /**
     * An index must be registered in order to be able to either subscribe to update events on the base
     * table and/or to provide Searcher functionality for reads. The double dispatch involved here, where
     * the Index actually performs its own registration by calling back to the supplied IndexRegistry's
     * own registerIndex method, is to make the decision as to whether or not to register an index belong
     * to the implementation, not the manager.
     * @param registry the index registry to register the instance with
     */
    public void register(IndexRegistry registry);

    /**
     * If the index implementation uses a local table to store its index data this method should return a
     * handle to it. If not, an empty Optional should be returned. Typically, this is useful for the built-in
     * Index implementations.
     * @return an Optional referencing the Index's backing storage table if it has one, or Optional.empty() if not.
     */
    public Optional<ColumnFamilyStore> getBackingTable();

    /**
     * Return a task which performs a blocking flush of the index's data to persistent storage.
     * @return task to be executed by the index manager to perform the flush.
     */
    public Callable<?> getBlockingFlushTask();

    /**
     * Return a task which invalidates the index, indicating it should no longer be considered usable.
     * This should include an clean up and releasing of resources required when dropping an index.
     * @return task to be executed by the index manager to invalidate the index.
     */
    public Callable<?> getInvalidateTask();

    /**
     * Return a task to truncate the index with the specified truncation timestamp.
     * Called when the base table is truncated.
     * @param truncatedAt timestamp of the truncation operation. This will be the same timestamp used
     *                    in the truncation of the base table.
     * @return task to be executed by the index manager when the base table is truncated.
     */
    public Callable<?> getTruncateTask(long truncatedAt);

    /**
     * Return a task to be executed before the node enters NORMAL state and finally joins the ring.
     *
     * @param hadBootstrap If the node had bootstrap before joining.
     * @return task to be executed by the index manager before joining the ring.
     */
    default public Callable<?> getPreJoinTask(boolean hadBootstrap)
    {
        return null;
    }

    /**
     * Return true if this index can be built or rebuilt when the index manager determines it is necessary. Returning
     * false enables the index implementation (or some other component) to control if and when SSTable data is
     * incorporated into the index.
     *
     * This is called by SecondaryIndexManager in buildIndexBlocking, buildAllIndexesBlocking and rebuildIndexesBlocking
     * where a return value of false causes the index to be exluded from the set of those which will process the
     * SSTable data.
     * @return if the index should be included in the set which processes SSTable data, false otherwise.
     */
    public boolean shouldBuildBlocking();

    /**
     * Get flush observer to observe partition/cell events generated by flushing SSTable (memtable flush or compaction).
     *
     * @param descriptor The descriptor of the sstable observer is requested for.
     * @param opType The type of the operation which requests observer e.g. memtable flush or compaction.
     *
     * @return SSTable flush observer.
     */
    default SSTableFlushObserver getFlushObserver(Descriptor descriptor, OperationType opType)
    {
        return null;
    }

    /*
     * Index selection
     */

    /**
     * Called to determine whether this index targets a specific column.
     * Used during schema operations such as when dropping or renaming a column, to check if
     * the index will be affected by the change. Typically, if an index answers that it does
     * depend upon a column, then schema operations on that column are not permitted until the index
     * is dropped or altered.
     *
     * @param column the column definition to check
     * @return true if the index depends on the supplied column being present; false if the column may be
     *              safely dropped or modified without adversely affecting the index
     */
    public boolean dependsOn(ColumnDefinition column);

    /**
     * Called to determine whether this index can provide a searcher to execute a query on the
     * supplied column using the specified operator. This forms part of the query validation done
     * before a CQL select statement is executed.
     * @param column the target column of a search query predicate
     * @param operator the operator of a search query predicate
     * @return true if this index is capable of supporting such expressions, false otherwise
     */
    public boolean supportsExpression(ColumnDefinition column, Operator operator);

    /**
     * If the index supports custom search expressions using the
     * {@code}SELECT * FROM table WHERE expr(index_name, expression){@code} syntax, this
     * method should return the expected type of the expression argument.
     * For example, if the index supports custom expressions as Strings, calls to this
     * method should return {@code}UTF8Type.instance{@code}.
     * If the index implementation does not support custom expressions, then it should
     * return null.
     * @return an the type of custom index expressions supported by this index, or an
     *         null if custom expressions are not supported.
     */
    public AbstractType<?> customExpressionValueType();

    /**
     * Transform an initial RowFilter into the filter that will still need to applied
     * to a set of Rows after the index has performed it's initial scan.
     * Used in ReadCommand#executeLocal to reduce the amount of filtering performed on the
     * results of the index query.
     *
     * @param filter the intial filter belonging to a ReadCommand
     * @return the (hopefully) reduced filter that would still need to be applied after
     *         the index was used to narrow the initial result set
     */
    public RowFilter getPostIndexQueryFilter(RowFilter filter);

    /**
     * Return an estimate of the number of results this index is expected to return for any given
     * query that it can be used to answer. Used in conjunction with indexes() and supportsExpression()
     * to determine the most selective index for a given ReadCommand. Additionally, this is also used
     * by StorageProxy.estimateResultsPerRange to calculate the initial concurrency factor for range requests
     *
     * @return the estimated average number of results a Searcher may return for any given query
     */
    public long getEstimatedResultRows();

    /*
     * Input validation
     */

    /**
     * Called at write time to ensure that values present in the update
     * are valid according to the rules of all registered indexes which
     * will process it. The partition key as well as the clustering and
     * cell values for each row in the update may be checked by index
     * implementations
     * @param update PartitionUpdate containing the values to be validated by registered Index implementations
     * @throws InvalidRequestException
     */
    public void validate(PartitionUpdate update) throws InvalidRequestException;

    /*
     * Update processing
     */

    /**
     * Creates an new {@code Indexer} object for updates to a given partition.
     *
     * @param key key of the partition being modified
     * @param columns the regular and static columns the created indexer will have to deal with.
     * This can be empty as an update might only contain partition, range and row deletions, but
     * the indexer is guaranteed to not get any cells for a column that is not part of {@code columns}.
     * @param nowInSec current time of the update operation
     * @param opGroup operation group spanning the update operation
     * @param transactionType indicates what kind of update is being performed on the base data
     *                        i.e. a write time insert/update/delete or the result of compaction
     * @return the newly created indexer or {@code null} if the index is not interested by the update
     * (this could be because the index doesn't care about that particular partition, doesn't care about
     * that type of transaction, ...).
     */
    public Indexer indexerFor(DecoratedKey key,
                              PartitionColumns columns,
                              int nowInSec,
                              OpOrder.Group opGroup,
                              IndexTransaction.Type transactionType);

    /**
     * Listener for processing events emitted during a single partition update.
     * Instances of this are responsible for applying modifications to the index in response to a single update
     * operation on a particular partition of the base table.
     *
     * That update may be generated by the normal write path, by iterating SSTables during streaming operations or when
     * building or rebuilding an index from source. Updates also occur during compaction when multiple versions of a
     * source partition from different SSTables are merged.
     *
     * Implementations should not make assumptions about resolution or filtering of the partition update being
     * processed. That is to say that it is possible for an Indexer instance to receive notification of a
     * PartitionDelete or RangeTombstones which shadow a Row it then receives via insertRow/updateRow.
     *
     * It is important to note that the only ordering guarantee made for the methods here is that the first call will
     * be to begin() and the last call to finish(). The other methods may be called to process update events in any
     * order. This can also include duplicate calls, in cases where a memtable partition is under contention from
     * several updates. In that scenario, the same set of events may be delivered to the Indexer as memtable update
     * which failed due to contention is re-applied.
     */
    public interface Indexer
    {
        /**
         * Notification of the start of a partition update.
         * This event always occurs before any other during the update.
         */
        public void begin();

        /**
         * Notification of a top level partition delete.
         * @param deletionTime
         */
        public void partitionDelete(DeletionTime deletionTime);

        /**
         * Notification of a RangeTombstone.
         * An update of a single partition may contain multiple RangeTombstones,
         * and a notification will be passed for each of them.
         * @param tombstone
         */
        public void rangeTombstone(RangeTombstone tombstone);

        /**
         * Notification that a new row was inserted into the Memtable holding the partition.
         * This only implies that the inserted row was not already present in the Memtable,
         * it *does not* guarantee that the row does not exist in an SSTable, potentially with
         * additional column data.
         *
         * @param row the Row being inserted into the base table's Memtable.
         */
        public void insertRow(Row row);

        /**
         * Notification of a modification to a row in the base table's Memtable.
         * This is allow an Index implementation to clean up entries for base data which is
         * never flushed to disk (and so will not be purged during compaction).
         * It's important to note that the old and new rows supplied here may not represent
         * the totality of the data for the Row with this particular Clustering. There may be
         * additional column data in SSTables which is not present in either the old or new row,
         * so implementations should be aware of that.
         * The supplied rows contain only column data which has actually been updated.
         * oldRowData contains only the columns which have been removed from the Row's
         * representation in the Memtable, while newRowData includes only new columns
         * which were not previously present. Any column data which is unchanged by
         * the update is not included.
         *
         * @param oldRowData data that was present in existing row and which has been removed from
         *                   the base table's Memtable
         * @param newRowData data that was not present in the existing row and is being inserted
         *                   into the base table's Memtable
         */
        public void updateRow(Row oldRowData, Row newRowData);

        /**
         * Notification that a row was removed from the partition.
         * Note that this is only called as part of either a compaction or a cleanup.
         * This context is indicated by the TransactionType supplied to the indexerFor method.
         *
         * As with updateRow, it cannot be guaranteed that all data belonging to the Clustering
         * of the supplied Row has been removed (although in the case of a cleanup, that is the
         * ultimate intention).
         * There may be data for the same row in other SSTables, so in this case Indexer implementations
         * should *not* assume that all traces of the row have been removed. In particular,
         * it is not safe to assert that all values associated with the Row's Clustering
         * have been deleted, so implementations which index primary key columns should not
         * purge those entries from their indexes.
         *
         * @param row data being removed from the base table
         */
        public void removeRow(Row row);

        /**
         * Notification of the end of the partition update.
         * This event always occurs after all others for the particular update.
         */
        public void finish();
    }

    /*
     * Querying
     */

    /**
     * Used to validate the various parameters of a supplied {@code}ReadCommand{@code},
     * this is called prior to execution. In theory, any command instance may be checked
     * by any {@code}Index{@code} instance, but in practice the index will be the one
     * returned by a call to the {@code}getIndex(ColumnFamilyStore cfs){@code} method on
     * the supplied command.
     *
     * Custom index implementations should perform any validation of query expressions here and throw a meaningful
     * InvalidRequestException when any expression or other parameter is invalid.
     *
     * @param command a ReadCommand whose parameters are to be verified
     * @throws InvalidRequestException if the details of the command fail to meet the
     *         index's validation rules
     */
    default void validate(ReadCommand command) throws InvalidRequestException
    {
    }

    /**
     * Return a function which performs post processing on the results of a partition range read command.
     * In future, this may be used as a generalized mechanism for transforming results on the coordinator prior
     * to returning them to the caller.
     *
     * This is used on the coordinator during execution of a range command to perform post
     * processing of merged results obtained from the necessary replicas. This is the only way in which results are
     * transformed in this way but this may change over time as usage is generalized.
     * See CASSANDRA-8717 for further discussion.
     *
     * The function takes a PartitionIterator of the results from the replicas which has already been collated
     * and reconciled, along with the command being executed. It returns another PartitionIterator containing the results
     * of the transformation (which may be the same as the input if the transformation is a no-op).
     */
    public BiFunction<PartitionIterator, ReadCommand, PartitionIterator> postProcessorFor(ReadCommand command);

    /**
     * Factory method for query time search helper.
     *
     * @param command the read command being executed
     * @return an Searcher with which to perform the supplied command
     */
    public Searcher searcherFor(ReadCommand command);

    /**
     * Performs the actual index lookup during execution of a ReadCommand.
     * An instance performs its query according to the RowFilter.Expression it was created for (see searcherFor)
     * An Expression is a predicate of the form [column] [operator] [value].
     */
    public interface Searcher
    {
        /**
         * @param executionController the collection of OpOrder.Groups which the ReadCommand is being performed under.
         * @return partitions from the base table matching the criteria of the search.
         */
        public UnfilteredPartitionIterator search(ReadExecutionController executionController);
    }
}
