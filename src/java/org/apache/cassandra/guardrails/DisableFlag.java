/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.guardrails;

import javax.annotation.Nullable;

import org.apache.cassandra.service.QueryState;

/**
 * A guardrail that completely disables the use of a particular feature.
 *
 * <p>Note that this guardrail only triggers failures (if the feature is disabled) so is only meant for
 * query-based guardrails (we're happy to reject queries deemed dangerous, but we don't want to create a guardrail
 * that breaks compaction for instance).
 */
public interface DisableFlag extends Guardrail
{
    /**
     * Triggers a failure if this guardrail is disabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    void ensureEnabled(@Nullable QueryState state);

    /**
     * Triggers a failure if this guardrail is disabled.
     *
     * <p>This must be called when the feature guarded by this guardrail is used to ensure such use is in fact
     * allowed.
     *
     * @param what  the feature that is guarded by this guardrail (for reporting in error messages).
     * @param state the query state, used to skip the check if the query is internal or is done by a superuser.
     *              A {@code null} value means that the check should be done regardless of the query.
     */
    void ensureEnabled(String what, @Nullable QueryState state);
}
