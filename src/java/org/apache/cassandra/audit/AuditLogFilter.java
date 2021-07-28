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
package org.apache.cassandra.audit;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AuditLogFilter
{
    private static final Logger logger = LoggerFactory.getLogger(AuditLogFilter.class);

    private static ImmutableSet<String> EMPTY_FILTERS = ImmutableSet.of();

    final ImmutableSet<String> excludedKeyspaces;
    final ImmutableSet<String> includedKeyspaces;
    final ImmutableSet<String> excludedCategories;
    final ImmutableSet<String> includedCategories;
    final ImmutableSet<String> includedUsers;
    final ImmutableSet<String> excludedUsers;

    private AuditLogFilter(ImmutableSet<String> excludedKeyspaces, ImmutableSet<String> includedKeyspaces, ImmutableSet<String> excludedCategories, ImmutableSet<String> includedCategories, ImmutableSet<String> excludedUsers, ImmutableSet<String> includedUsers)
    {
        this.excludedKeyspaces = excludedKeyspaces;
        this.includedKeyspaces = includedKeyspaces;
        this.excludedCategories = excludedCategories;
        this.includedCategories = includedCategories;
        this.includedUsers = includedUsers;
        this.excludedUsers = excludedUsers;
    }

    /**
     * (Re-)Loads filters from config. Called during startup as well as JMX invocations.
     */
    public static AuditLogFilter create(AuditLogOptions auditLogOptions)
    {
        logger.trace("Loading AuditLog filters");

        IncludeExcludeHolder keyspaces = loadInputSets(auditLogOptions.included_keyspaces, auditLogOptions.excluded_keyspaces);
        IncludeExcludeHolder categories = loadInputSets(auditLogOptions.included_categories, auditLogOptions.excluded_categories);
        IncludeExcludeHolder users = loadInputSets(auditLogOptions.included_users, auditLogOptions.excluded_users);

        return new AuditLogFilter(keyspaces.excludedSet, keyspaces.includedSet,
                                  categories.excludedSet, categories.includedSet,
                                  users.excludedSet, users.includedSet);
    }

    /**
     * Constructs mutually exclusive sets of included and excluded data. When there is a conflict,
     * the entry is put into the excluded set (and removed fron the included).
     */
    private static IncludeExcludeHolder loadInputSets(String includedInput, String excludedInput)
    {
        final ImmutableSet<String> excludedSet;
        if (StringUtils.isEmpty(excludedInput))
        {
            excludedSet = EMPTY_FILTERS;
        }
        else
        {
            String[] excludes = excludedInput.split(",");
            ImmutableSet.Builder<String> builder = ImmutableSet.builderWithExpectedSize(excludes.length);
            for (String exclude : excludes)
            {
                if (!exclude.isEmpty())
                {
                    builder.add(exclude);
                }
            }
            excludedSet = builder.build();
        }

        final ImmutableSet<String> includedSet;
        if (StringUtils.isEmpty(includedInput))
        {
            includedSet = EMPTY_FILTERS;
        }
        else
        {
            String[] includes = includedInput.split(",");
            ImmutableSet.Builder<String> builder = ImmutableSet.builderWithExpectedSize(includes.length);
            for (String include : includes)
            {
                //Ensure both included and excluded sets are mutually exclusive
                if (!include.isEmpty() && !excludedSet.contains(include))
                {
                    builder.add(include);
                }
            }
            includedSet = builder.build();
        }

        return new IncludeExcludeHolder(includedSet, excludedSet);
    }

    /**
     * Simple struct to hold inclusion/exclusion sets.
     */
    private static class IncludeExcludeHolder
    {
        private final ImmutableSet<String> includedSet;
        private final ImmutableSet<String> excludedSet;

        private IncludeExcludeHolder(ImmutableSet<String> includedSet, ImmutableSet<String> excludedSet)
        {
            this.includedSet = includedSet;
            this.excludedSet = excludedSet;
        }
    }

    /**
     * Checks whether a give AuditLog Entry is filtered or not
     *
     * @param auditLogEntry AuditLogEntry to verify
     * @return true if it is filtered, false otherwise
     */
    boolean isFiltered(AuditLogEntry auditLogEntry)
    {
        return isFiltered(auditLogEntry.getKeyspace(), includedKeyspaces, excludedKeyspaces)
               || isFiltered(auditLogEntry.getType().getCategory().toString(), includedCategories, excludedCategories)
               || isFiltered(auditLogEntry.getUser(), includedUsers, excludedUsers);
    }

    /**
     * Checks whether given input is being filtered or not.
     * If excludeSet does not contain any items, by default nothing is excluded (unless there are
     * entries in the includeSet).
     * If includeSet does not contain any items, by default everything is included
     * If an input is part of both includeSet and excludeSet, excludeSet takes the priority over includeSet
     *
     * @param input      Input to be checked for filtereing based on includeSet and excludeSet
     * @param includeSet Include filtering set
     * @param excludeSet Exclude filtering set
     * @return true if the input is filtered, false when the input is not filtered
     */
    static boolean isFiltered(String input, Set<String> includeSet, Set<String> excludeSet)
    {
        if (!excludeSet.isEmpty() && excludeSet.contains(input))
            return true;

        return !(includeSet.isEmpty() || includeSet.contains(input));
    }
}
