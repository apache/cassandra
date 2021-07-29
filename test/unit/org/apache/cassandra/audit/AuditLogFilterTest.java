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

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.audit.AuditLogFilter.isFiltered;

public class AuditLogFilterTest
{
    @Test
    public void testInputWithSpaces()
    {
        AuditLogOptions auditLogOptions = new AuditLogOptions.Builder()
                                          .withIncludedKeyspaces(" ks, ks1, ks3, ")
                                          .withEnabled(true)
                                          .build();

        AuditLogFilter auditLogFilter = AuditLogFilter.create(auditLogOptions);

        Assert.assertFalse(auditLogFilter.isFiltered(new AuditLogEntry.Builder(AuditLogEntryType.CREATE_TYPE).setKeyspace("ks").build()));
        Assert.assertFalse(auditLogFilter.isFiltered(new AuditLogEntry.Builder(AuditLogEntryType.CREATE_TYPE).setKeyspace("ks1").build()));
        Assert.assertFalse(auditLogFilter.isFiltered(new AuditLogEntry.Builder(AuditLogEntryType.CREATE_TYPE).setKeyspace("ks3").build()));
        Assert.assertTrue(auditLogFilter.isFiltered(new AuditLogEntry.Builder(AuditLogEntryType.CREATE_TYPE).setKeyspace("ks5").build()));
    }

    @Test
    public void isFiltered_IncludeSetOnly()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();

        Assert.assertFalse(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("d", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_ExcludeSetOnly()
    {
        Set<String> includeSet = new HashSet<>();

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");
        excludeSet.add("b");
        excludeSet.add("c");

        Assert.assertTrue(isFiltered("a", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("b", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("c", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("d", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_MutualExclusive()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_MutualInclusive()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("c");
        excludeSet.add("d");

        Assert.assertFalse(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("d", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("e", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("f", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_UnSpecifiedInput()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("d", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_SpecifiedInput()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("c", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_FilteredInput_EmptyInclude()
    {
        Set<String> includeSet = new HashSet<>();
        Set<String> excludeSet = new HashSet<>();
        excludeSet.add("a");

        Assert.assertTrue(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_FilteredInput_EmptyExclude()
    {
        Set<String> includeSet = new HashSet<>();
        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");

        Set<String> excludeSet = new HashSet<>();

        Assert.assertFalse(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("b", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("c", includeSet, excludeSet));
        Assert.assertTrue(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_EmptyInputs()
    {
        Set<String> includeSet = new HashSet<>();
        Set<String> excludeSet = new HashSet<>();

        Assert.assertFalse(isFiltered("a", includeSet, excludeSet));
        Assert.assertFalse(isFiltered("e", includeSet, excludeSet));
    }

    @Test
    public void isFiltered_NullInputs()
    {
        Set<String> includeSet = new HashSet<>();
        Set<String> excludeSet = new HashSet<>();
        Assert.assertFalse(isFiltered(null, includeSet, excludeSet));

        includeSet.add("a");
        includeSet.add("b");
        includeSet.add("c");
        Assert.assertTrue(isFiltered(null, includeSet, excludeSet));

        includeSet = new HashSet<>();
        excludeSet.add("a");
        excludeSet.add("b");
        Assert.assertFalse(isFiltered(null, includeSet, excludeSet));
    }
}
