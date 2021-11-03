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

package org.apache.cassandra.db.guardrails;

/**
 * Configuration settings for guardrails.
 *
 * <p>Note that the settings here must only be used by the {@link Guardrails} class and not directly by the code
 * checking each guarded constraint (which, again, should use the higher level abstractions defined in
 * {@link Guardrails}).
 *
 * <p>This contains a main setting, {@code enabled}, controlling if guardrails are globally active or not, and
 * individual settings to control each guardrail.
 *
 * <p>We have 2 variants of guardrails, soft (warn) and hard (abort) limits, each guardrail having either one of the
 * variants or both. Note in particular that hard limits only make sense for guardrails triggering during query
 * execution. For other guardrails, say one triggering during compaction, aborting that compaction does not make sense.
 *
 * <p>Additionally, each individual setting should have a specific value (typically -1 for numeric settings),
 * that allows to disable the corresponding guardrail.
 * <p>
 * This configuration is offered as an interface so different implementations of {@link GuardrailsConfigProvider} can
 * provide different implementations of this config. However, this mechanism for guardrails config pluggability is not
 * officially supported and this interface may change in a minor release.
 */
public interface GuardrailsConfig
{
    /**
     * Whether guardrails are enabled or not.
     *
     * @return {@code true} if guardrails are enabled, {@code false} otherwise
     */
    boolean getEnabled();

    /**
     * @return The threshold to warn or abort when creating more tables than threshold.
     */
    Threshold.Config getTables();

    /**
     * @return The threshold to warn or abort when creating more columns per table than threshold.
     */
    Threshold.Config getColumnsPerTable();

    /**
     * @return The threshold to warn or abort when creating more secondary indexes per table than threshold.
     */
    Threshold.Config getSecondaryIndexesPerTable();

    /**
     * @return The threshold to warn or abort when creating more materialized views per table than threshold.
     */
    Threshold.Config getMaterializedViewsPerTable();

    /**
     * @return The table properties that are ignored/disallowed when creating or altering a table.
     */
    Values.Config<String> getTableProperties();

    /**
     * Returns whether user-provided timestamps are allowed.
     *
     * @return {@code true} if user-provided timestamps are allowed, {@code false} otherwise.
     */
    boolean getUserTimestampsEnabled();
}
