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
package org.apache.cassandra.index.sai.disk.v1.segment;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;

import static org.apache.cassandra.index.sai.SAITester.getRandom;
import static org.junit.Assert.assertEquals;

/**
 * Tests for prefix search optimization when query prefix lands at different depths
 * relative to eligible prefix posting depths (multiples of SAI_POSTINGS_SKIP).
 */
public class PrefixSectionOptimizationTest extends SAITester
{
    @Before
    public void setup()
    {
        // Ensure skip=3 and minimum=64 for predictable test behavior
        CassandraRelevantProperties.SAI_POSTINGS_SKIP.setString("3");
        CassandraRelevantProperties.SAI_MINIMUM_POSTINGS_LEAVES.setString("64");
    }

    /**
     * Test: Query prefix at depth 0 (root) with prefix section.
     * Expected: Fast path - single I/O reading exact+prefix section.
     */
    @Test
    public void testPrefixAtDepth0WithSection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Insert enough rows to trigger prefix section at root (depth 0 % 3 == 0)
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "term_" + i);
        
        flush();

        // Query with single-char prefix that covers all rows
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "t%"), 100);
    }

    /**
     * Test: Query prefix at depth 3 (eligible) with prefix section.
     * Expected: Fast path - single I/O.
     */
    @Test
    public void testPrefixAtDepth3WithSection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Create 100 terms starting with "abc" (depth 3 = eligible)
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "abc_" + i);
        
        flush();

        // Query 'abc%' - should hit fast path
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "abc%"), 100);
    }

    /**
     * Test: Query prefix at depth 2 (ineligible), children at depth 3 have sections.
     * Expected: Smart fallback - read children's exact+prefix sections.
     */
    @Test
    public void testPrefixAtDepth2ChildrenHaveSections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Create two groups at depth 3, each with >64 rows
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "aba_" + i);  // "ab" depth 2, "aba" depth 3
        
        for (int i = 80; i < 160; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "abb_" + i); // "ab" depth 2, "abb" depth 3
        
        flush();

        // Query 'ab%' (depth 2, no section) - should read 'aba' and 'abb' sections
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "ab%"), 160);
    }

    /**
     * Test: Query prefix at depth 2, children at depth 3 do NOT have sections (too few rows).
     * Expected: Smart fallback reads exact sections for each leaf term.
     */
    @Test
    public void testPrefixAtDepth2ChildrenNoSections() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Create small groups at depth 3, each with <64 rows (no prefix sections)
        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "aba_" + i);
        
        for (int i = 10; i < 20; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "abb_" + i);
        
        flush();

        // Query 'ab%' - should read exact sections from leaves
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "ab%"), 20);
    }

    /**
     * Test: Mixed scenario - some children have sections, some don't.
     * Expected: Read sections where available, exact-only for leaves.
     */
    @Test
    public void testMixedChildrenSomeSectionsNotAll() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Group 1: >64 rows (will have prefix section at depth 3)
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "pra_" + i);
        
        // Group 2: <64 rows (no prefix section)
        for (int i = 80; i < 90; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "prb_" + i);
        
        flush();

        // Query 'pr%' - should use section for 'pra', exact-only for 'prb' terms
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "pr%"), 90);
    }

    /**
     * Test: Deep nesting - query at depth 1, sections at depth 6.
     * Expected: Smart fallback navigates through intermediate depths to find sections.
     */
    @Test
    public void testDeepNestingQueryDepth1SectionsAtDepth6() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Create terms at depth 6 with >64 rows each
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "xabcde_" + i);  // depth 6
        
        for (int i = 80; i < 160; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "xabcdf_" + i); // depth 6
        
        flush();

        // Query 'x%' (depth 1) - should find and use sections at depth 6
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "x%"), 160);
    }

    /**
     * Test: No matching terms.
     * Expected: Returns empty result without errors.
     */
    @Test
    public void testNoMatchingTerms() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "apple_" + i);
        
        flush();

        // Query for non-existent prefix
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "banana%"), 0);
    }

    /**
     * Test: Single matching term with prefix section.
     * Expected: Returns correct result.
     */
    @Test
    public void testSingleTermWithPrefixSection() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Single term repeated enough times to create prefix section
        for (int i = 0; i < 100; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "unique");
        
        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "uniq%"), 100);
    }

    /**
     * Test: Multiple SSTable segments with different prefix structures.
     * Expected: Correctly merges results across segments.
     */
    @Test
    public void testMultipleSegmentsWithDifferentStructures() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Segment 1: Many terms starting with "seg1_" (will have sections)
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "seg1_abc" + i);
        flush();

        // Segment 2: Few terms starting with "seg1_" (no sections)
        for (int i = 1000; i < 1010; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "seg1_xyz" + i);
        flush();

        // Query should merge results from both segments
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "seg1_%"), 90);
    }

    /**
     * Test: Prefix search with memtable + SSTable.
     * Expected: Correctly merges memtable (no sections) and SSTable (with sections).
     */
    @Test
    public void testMemtableAndSSTableMerge() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // SSTable: many rows (will have prefix sections)
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "merged_" + i);
        flush();

        // Memtable: additional rows (no sections yet)
        for (int i = 1000; i < 1020; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "merged_" + i);

        // Query should merge both sources
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "merged_%"), 100);
    }

    /**
     * Test: Exact match vs prefix match behavior.
     * Expected: Exact match uses exact section only, prefix match uses exact+prefix.
     */
    @Test
    public void testExactMatchVsPrefixMatch() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        execute("INSERT INTO %s (pk, value) VALUES (?, ?)", 1, "exact");
        for (int i = 10; i < 90; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "exact_" + i);
        
        flush();

        // Exact match: should return only "exact"
        assertRowCount(execute("SELECT * FROM %s WHERE value = ?", "exact"), 1);

        // Prefix match: should return "exact" + all "exact_*"
        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ?", "exact%"), 81);
    }

    /**
     * Test: Special characters in prefix.
     * Expected: Handles special characters correctly.
     */
    @Test
    public void testSpecialCharactersInPrefix() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "test-value_" + i);
        
        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ? ALLOW FILTERING", "test-value%"), 80);
    }

    /**
     * Test: Very long prefix (many characters).
     * Expected: Handles deep trie paths correctly.
     */
    @Test
    public void testVeryLongPrefix() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        String longPrefix = "verylongprefixstring";
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, longPrefix + "_" + i);
        
        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ? ALLOW FILTERING", longPrefix + "%"), 80);
    }

    /**
     * Test: Prefix with all 'z' characters (edge case for successor computation).
     * Expected: Handles unbounded upper range correctly.
     */
    @Test
    public void testPrefixWithMaxBytes() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Note: Most string values don't contain 0xFF bytes, so this tests normal behavior
        for (int i = 0; i < 80; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, "zzz_" + i);
        
        flush();

        assertRowCount(execute("SELECT * FROM %s WHERE value LIKE ? ALLOW FILTERING", "zzz%"), 80);
    }

    /**
     * Verifies that the memtable prefix flush fast path (which resolves prefix postings directly from
     * the {@link org.apache.cassandra.index.sai.memory.TrieMemoryIndex} via
     * {@code RowMapping.mergeV2}, eliminating the intermediate {@code SegmentTrieBuffer} rebuild)
     * produces query results identical to the pre-flush in-memory path for the same data.
     */
    @Test
    public void testMemtablePrefixFlushMatchesMemtablePath() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, value text)");
        createIndex(String.format("CREATE INDEX ON %%s(value) USING 'sai' WITH OPTIONS = {'%s': 'true'}",
                                   IndexWriterConfig.ENABLE_LITERAL_PREFIX_SAI));

        // Ten groups "prod00".."prod09", 30 rows each: "prod0X" reaches depth 6 (eligible for a prefix
        // section) while each group individually is below the 64-row minimum, exercising both the
        // exact and (pruned) prefix sections of the V2 flush path.
        for (int i = 0; i < 300; i++)
            execute("INSERT INTO %s (pk, value) VALUES (?, ?)", i, String.format("prod%02d_item_%04d", i % 10, i));

        // Query while the data still lives only in the memtable (in-memory search path).
        long memtableGroupCount = execute("SELECT pk FROM %s WHERE value LIKE ?", "prod05%").size();
        long memtableAllCount = execute("SELECT pk FROM %s WHERE value LIKE ?", "prod%").size();
        assertEquals(30, memtableGroupCount);
        assertEquals(300, memtableAllCount);

        // Flush through the optimized prefix path, then re-run the same queries against the SSTable.
        flush();
        waitForTableIndexesQueryable();

        assertRowCount(execute("SELECT pk FROM %s WHERE value LIKE ?", "prod05%"), (int) memtableGroupCount);
        assertRowCount(execute("SELECT pk FROM %s WHERE value LIKE ?", "prod%"), (int) memtableAllCount);

        // A single group is below the prefix-section minimum, so its section is pruned but exact
        // postings must still return every row.
        assertRowCount(execute("SELECT pk FROM %s WHERE value = ?", String.format("prod00_item_%04d", 0)), 1);
    }
}
