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

package org.apache.cassandra.db.compaction.differential;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Checks that {@link #assertEquivalentOutputs} fails when one output is wrong.
 *
 * <p>It runs no compaction. It builds {@link CapturedSSTable} objects from the component files of one
 * flushed sstable, then changes a copy in one of these ways:
 * <ul>
 *   <li>a flipped byte in each component;</li>
 *   <li>a dropped row in the logical dump;</li>
 *   <li>a changed stat;</li>
 *   <li>a missing component.</li>
 * </ul>
 * Each change must raise an {@link AssertionError}.
 */
@RunWith(Parameterized.class)
public class DifferentialComparisonCatchesFaultsTest extends DifferentialCompactionTester
{
    @Parameterized.Parameter
    public String format;

    @Parameterized.Parameters(name = "format={0}")
    public static Collection<Object[]> formats()
    {
        return List.of(new Object[]{ "big" }, new Object[]{ "bti" });
    }

    // A small, non-digest logical dump. Non-digest so assertEquivalentSSTable compares it line by line.
    private static final String CANON_JSON = "[\n  {\"partition\":{\"key\":[\"0\"]}},\n" +
                                             "  {\"rows\":[{\"clustering\":[\"0\"],\"value\":\"a\"}]},\n" +
                                             "  {\"rows\":[{\"clustering\":[\"1\"],\"value\":\"b\"}]}\n]\n";
    private static final String CANON_STATS = "minTimestamp=1000 maxTimestamp=2000 totalRows=2";

    private Path scratch;
    private ColumnFamilyStore cfs;
    private SSTableReader sstable;

    @Before
    public void setUp() throws IOException
    {
        selectSSTableFormat(format);
        scratch = Files.createTempDirectory("differential-negative-control");

        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck))");
        cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        for (long ck = 0; ck < 8; ck++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", 0L, ck, "row-" + ck);
        flush();

        sstable = cfs.getLiveSSTables().iterator().next();
    }

    @After
    public void tearDown() throws IOException
    {
        restoreSelectedFormat();
        if (scratch != null)
            org.apache.commons.io.FileUtils.deleteDirectory(scratch.toFile());
    }

    /** The comparison must PASS when both captures are byte-identical; otherwise every real run is a false red. */
    @Test
    public void identicalCapturesAreEquivalent() throws IOException
    {
        CapturedOutput a = outputOf(capture("a", CANON_JSON, CANON_STATS, null));
        CapturedOutput b = outputOf(capture("b", CANON_JSON, CANON_STATS, null));
        assertEquivalentOutputs(a, b); // must not throw
    }

    /** A single flipped byte in ANY component must be caught, including the Data.db payload. */
    @Test
    public void byteFlipInEveryComponentIsCaught() throws IOException
    {
        List<String> components = componentNames();
        assertTrue("expected the flushed sstable to have a Data.db component to perturb",
                   components.stream().anyMatch(c -> c.endsWith("Data.db")));

        int perturbed = 0;
        for (String comp : components)
        {
            CapturedSSTable good = capture("good-" + comp, CANON_JSON, CANON_STATS, null);
            CapturedSSTable flipped = capture("flip-" + comp, CANON_JSON, CANON_STATS, comp);
            if (flipped == null) // the component file was empty; nothing to flip
                continue;
            assertCaught("a flipped byte in component " + comp, outputOf(good), outputOf(flipped));
            perturbed++;
        }
        assertTrue("no component had bytes to flip", perturbed > 0);
    }

    /** A dropped row in the logical dump must be caught before the byte comparison even runs. */
    @Test
    public void droppedRowInLogicalDumpIsCaught() throws IOException
    {
        String dumpMinusOneRow = CANON_JSON.replace("  {\"rows\":[{\"clustering\":[\"1\"],\"value\":\"b\"}]}\n", "");
        assertFalse("test setup error: the dropped-row dump is identical to the canonical dump",
                    dumpMinusOneRow.equals(CANON_JSON));
        CapturedSSTable good = capture("logical-good", CANON_JSON, CANON_STATS, null);
        CapturedSSTable dropped = capture("logical-dropped", dumpMinusOneRow, CANON_STATS, null);
        assertCaught("a dropped row in the logical dump", outputOf(good), outputOf(dropped));
    }

    /** A single perturbed stat must be caught even when the logical dump and every byte match. */
    @Test
    public void perturbedStatIsCaught() throws IOException
    {
        String perturbedStats = CANON_STATS.replace("totalRows=2", "totalRows=3");
        CapturedSSTable good = capture("stat-good", CANON_JSON, CANON_STATS, null);
        CapturedSSTable perturbed = capture("stat-perturbed", CANON_JSON, perturbedStats, null);
        assertCaught("a perturbed stat summary", outputOf(good), outputOf(perturbed));
    }

    /** A component present in one path but not the other must be caught. */
    @Test
    public void missingComponentIsCaught() throws IOException
    {
        List<String> components = componentNames();
        CapturedSSTable good = capture("missing-good", CANON_JSON, CANON_STATS, null);
        CapturedSSTable missing = capture("missing-one", CANON_JSON, CANON_STATS, null);

        // drop one component from the second capture: remove both its size entry and its file
        String drop = components.get(0);
        missing.componentSizes.remove(drop);
        Files.delete(missing.dir.resolve(drop));

        assertCaught("a component (" + drop + ") present in only one path", outputOf(good), outputOf(missing));
    }

    /**
     * Builds a {@link CapturedSSTable} from the real component files of the flushed sstable, copied into a
     * fresh scratch directory. If {@code flipComponent} is non-null, one byte of that component's copy is
     * flipped; returns null if that component's file is empty and cannot be perturbed.
     */
    private CapturedSSTable capture(String label, String json, String statsSummary, String flipComponent) throws IOException
    {
        Path dir = scratch.resolve(label);
        Files.createDirectories(dir);
        CapturedSSTable captured = new CapturedSSTable(dir, json, statsSummary, 2);
        for (Component c : sstable.descriptor.discoverComponents())
        {
            Path source = sstable.descriptor.fileFor(c).toPath();
            Path target = dir.resolve(c.name());
            Files.copy(source, target);
            if (c.name().equals(flipComponent))
            {
                byte[] bytes = Files.readAllBytes(target);
                if (bytes.length == 0)
                    return null;
                bytes[bytes.length / 2] ^= (byte) 0xFF;
                Files.write(target, bytes);
            }
            captured.componentSizes.put(c.name(), Files.size(target));
        }
        return captured;
    }

    /** The names of every component the flushed sstable carries. */
    private List<String> componentNames()
    {
        List<String> names = new ArrayList<>();
        for (Component c : sstable.descriptor.discoverComponents())
            names.add(c.name());
        return names;
    }

    private static CapturedOutput outputOf(CapturedSSTable s)
    {
        CapturedOutput out = new CapturedOutput();
        out.sstables.add(s);
        return out;
    }

    /**
     * Asserts the differential comparison rejects the given pair. Any {@link AssertionError} from
     * {@link #assertEquivalentOutputs} is the expected outcome; the final assertion is outside the try so
     * it is not itself swallowed as a "caught" divergence.
     */
    private void assertCaught(String what, CapturedOutput a, CapturedOutput b)
    {
        boolean caught = false;
        try
        {
            assertEquivalentOutputs(a, b);
        }
        catch (AssertionError expected)
        {
            caught = true;
        }
        assertTrue("the differential comparison did not catch " + what +
                   "; the comparison has a blind spot and every green run through it is suspect", caught);
    }
}
