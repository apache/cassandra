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

package org.apache.cassandra.guardrails;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.guardrails.Guardrail.DisableFlag;
import org.apache.cassandra.guardrails.Guardrail.DisallowedValues;
import org.apache.cassandra.guardrails.Guardrail.PercentageThreshold;
import org.apache.cassandra.guardrails.Guardrail.Predicates;
import org.apache.cassandra.guardrails.Guardrail.SizeThreshold;
import org.apache.cassandra.guardrails.Guardrail.Threshold;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.disk.usage.DiskUsageBroadcaster;

import static java.lang.String.format;

/**
 * Entry point for Guardrails, storing the defined guardrails and provided a few global methods over them.
 */
public abstract class Guardrails
{
    private static final GuardrailsConfig config = DatabaseDescriptor.getGuardrailsConfig();

    public static final Threshold columnValueSize = new SizeThreshold("column_value_size",
                                                                      () -> -1L, // not needed so far
                                                                      () -> config.column_value_size_failure_threshold_in_kb * 1024L,
                                                                      (x, what, v, t) -> format("Value of %s of size %s is greater than the maximum allowed (%s)",
                                                                                                what, v, t));

    public static final Threshold columnsPerTable = new Threshold("columns_per_table",
                                                                  () -> -1L, // not needed so far
                                                                  () -> config.columns_per_table_failure_threshold,
                                                                  (x, what, v, t) -> format("Tables cannot have more than %s columns, but %s provided for table %s",
                                                                                            t, v, what));

    public static final DisableFlag userTimestampsEnabled = new DisableFlag("user_provided_timestamps",
                                                                            () -> !config.user_timestamps_enabled,
                                                                            "User provided timestamps (USING TIMESTAMP)");

    public static final DisallowedValues<ConsistencyLevel> disallowedWriteConsistencies = new DisallowedValues<>("disallowed_write_consistency_levels",
                                                                                                                 () -> config.write_consistency_levels_disallowed,
                                                                                                                 ConsistencyLevel::fromString,
                                                                                                                 "Consistency Level");

    public static final Threshold secondaryIndexesPerTable = new Threshold("secondary_indexes_per_table",
                                                                           () -> -1,
                                                                           () -> config.secondary_index_per_table_failure_threshold,
                                                                           (x, what, v, t) -> format("Tables cannot have more than %s secondary indexes, failed to create secondary index %s",
                                                                                                     t, what));

    public static final Threshold materializedViewsPerTable = new Threshold("materialized_views_per_table",
                                                                            () -> -1,
                                                                            () -> config.materialized_view_per_table_failure_threshold,
                                                                            (x, what, v, t) -> format("Tables cannot have more than %s materialized views, failed to create materialized view %s",
                                                                                                      t, what));

    public static final Threshold tablesLimit = new Threshold("number_of_tables",
                                                              () -> config.tables_warn_threshold,
                                                              () -> config.tables_failure_threshold,
                                                              (isWarning, what, v, t) -> isWarning
                                                                                         ? format("Creating table %s, current number of tables %s exceeds warning threshold of %s.",
                                                                                                  what, v, t)
                                                                                         : format("Cannot have more than %s tables, failed to create table %s",
                                                                                                  t, what));

    public static final DisallowedValues<String> disallowedTableProperties = new DisallowedValues<>("disallowed_table_properties",
                                                                                                    () -> config.table_properties_disallowed,
                                                                                                    String::toLowerCase,
                                                                                                    "Table Properties");

    @SuppressWarnings("unchecked")
    public static final Predicates<InetAddressAndPort> replicaDiskUsage =
    (Predicates<InetAddressAndPort>) new Predicates<>("replica_disk_usage",
                                                      DiskUsageBroadcaster.instance::isStuffed,
                                                      DiskUsageBroadcaster.instance::isFull,
                                                      // not using `what` because it represents replica address which should be hidden from client.
                                                      (isWarning, what) -> isWarning
                                                                           ? "Replica disk usage exceeds warn threshold"
                                                                           : "Write request failed because disk usage exceeds failure threshold")
                                     .minNotifyIntervalInMs(TimeUnit.MINUTES.toMillis(30));

    public static final PercentageThreshold localDiskUsage =
    (PercentageThreshold) new PercentageThreshold("local_disk_usage",
                                                  () -> config.disk_usage_percentage_warn_threshold,
                                                  () -> config.disk_usage_percentage_failure_threshold,
                                                  (isWarning, what, v, t) -> isWarning
                                                                             ? format("Local disk usage %s(%s) exceeds warn threshold of %s", v, what, t)
                                                                             : format("Local disk usage %s(%s) exceeds failure threshold of %s, will stop accepting writes", v, what, t))
                          .noExceptionOnFailure()
                          .minNotifyIntervalInMs(TimeUnit.MINUTES.toMillis(30));

    public static final Threshold partitionSize =
    new SizeThreshold("partition_size",
                      () -> config.partition_size_warn_threshold_in_mb * 1024L * 1024L,
                      () -> -1L,
                      (x, what, v, t) -> format("Detected partition %s of size %s is greater than the maximum recommended size (%s)",
                                                what, v, t));

    public static final Threshold partitionKeysInSelectQuery =
    new Threshold("partition_keys_in_select_query",
                  () -> -1L,
                  () -> config.partition_keys_in_select_failure_threshold,
                  (x, what, v, t) -> format("%s cannot be completed because it selects %s partitions keys - more than the maximum allowed %s", what, v, t));

    public static final Threshold fieldsPerUDT =
    new Threshold("fields_per_udt",
                  () -> -1L, // not needed so far
                  () -> config.fields_per_udt_failure_threshold,
                  (x, what, v, t) -> format("User types cannot have more than %s columns, but %s provided for type %s",
                                            t, v, what));

    public static final Threshold collectionSize =
    new SizeThreshold("collection_size",
                      () -> config.collection_size_warn_threshold_in_kb * 1024L,
                      () -> -1L, // not needed so far
                      (x, what, v, t) -> format("Detected collection %s of size %s, greater than the maximum recommended size (%s)",
                                                what, v, t));

    public static final Threshold itemsPerCollection =
    new Threshold("items_per_collection",
                  () -> config.items_per_collection_warn_threshold,
                  () -> -1L, // not needed so far
                  (x, what, v, t) -> format("Detected collection %s with %s items, greater than the maximum recommended (%s)",
                                            what, v, t));

    public static final Threshold inSelectCartesianProduct =
    new Threshold("in_select_cartesian_product",
                  () -> -1L,
                  () -> config.in_select_cartesian_product_failure_threshold,
                  (x, what, v, t) -> format("The query cannot be completed because cartesian product of all values in IN conditions is greater than %s", t));

    public static final DisableFlag readBeforeWriteListOperationsEnabled =
    new DisableFlag("read_before_write_list_operations",
                    () -> !config.read_before_write_list_operations_enabled,
                    "List operation requiring read before write");

    static final List<Listener> listeners = new CopyOnWriteArrayList<>();

    private Guardrails()
    {
    }

    /**
     * Whether guardrails are enabled globally or not.
     *
     * @return {@code true} if guardrails are enabled (applies based on their individual setting), {@code false}
     * otherwise (in which case no guardrail will trigger).
     */
    public static boolean enabled()
    {
        return config.enabled;
    }

    /**
     * Whether guardrails are ready.
     *
     * @return {@code true} if daemon is initialized (applies based on their individual setting), {@code false}
     * otherwise (in which case no guardrail will trigger).
     */
    public static boolean ready()
    {
        return DatabaseDescriptor.isDaemonInitialized();
    }

    /**
     * Register a {@link Listener}.
     *
     * <p>Note that listeners are called in the order they are registered, and on the thread on which the guardrail
     * is triggered.
     *
     * @param listener the listener to register. If the same listener is registered twice (or more), its method will be
     *                 called twice (or more) for every trigger.
     */
    public static void register(Listener listener)
    {
        listeners.add(listener);
    }

    /**
     * Unregister a previously registered listener.
     *
     * @param listener the listener to unregister. If it was not registered before, this is a no-op. If it was
     *                 registered more than once, only one of the instance is unregistered.
     */
    public static void unregister(Listener listener)
    {
        listeners.remove(listener);
    }

    /**
     * Interface for external listeners interested in being notified when a guardrail is triggered.
     *
     * <p>Listeners should be registered through the {@link #register} method to take effect.
     */
    public interface Listener
    {
        /**
         * Called when a guardrail triggers a warning.
         *
         * @param guardrailName a name describing the guardrail.
         * @param message       the message corresponding to the guardrail trigger.
         */
        public void onWarningTriggered(String guardrailName, String message);

        /**
         * Called when a guardrail triggers a failure.
         *
         * @param guardrailName a name describing the guardrail.
         * @param message       the message corresponding to the guardrail trigger.
         */
        public void onFailureTriggered(String guardrailName, String message);
    }
}