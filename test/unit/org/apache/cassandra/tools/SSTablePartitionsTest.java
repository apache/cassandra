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

package org.apache.cassandra.tools;

import java.io.IOException;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.ToolRunner.ToolResult;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Tests for {@link SSTablePartitions}.
 */
public class SSTablePartitionsTest extends OfflineToolUtils
{
    private static final String SSTABLE_1 = sstable("legacy_ma_simple");
    private static final String SSTABLE_2 = sstable("legacy_ma_clust");
    private static final String HEADER_1 = "\nProcessing  #1 (big-ma) (173 B uncompressed, 88 B on disk)\n";
    private static final String HEADER_2 = "\nProcessing  #1 (big-ma) (328.145 KiB uncompressed, 5.096 KiB on disk)\n";
    private static final String BACKUPS_HEADER_1 = "\nProcessing Backup:backups  #1 (big-ma) (173 B uncompressed, 88 B on disk)\n";
    private static final String BACKUPS_HEADER_2 = "\nProcessing Backup:backups  #1 (big-ma) (328.145 KiB uncompressed, 5.096 KiB on disk)\n";
    private static final String SNAPSHOTS_HEADER_1 = "\nProcessing Snapshot:snapshot-1  #1 (big-ma) (173 B uncompressed, 88 B on disk)\n";
    private static final String SNAPSHOTS_HEADER_2 = "\nProcessing Snapshot:snapshot-1  #1 (big-ma) (328.145 KiB uncompressed, 5.096 KiB on disk)\n";
    private static final String SUMMARY_1 = "               Partition size            Row count           Cell count      Tombstone count\n" +
                                            "  ~p50                   35 B                    1                    1                    0\n" +
                                            "  ~p75                   35 B                    1                    1                    0\n" +
                                            "  ~p90                   35 B                    1                    1                    0\n" +
                                            "  ~p95                   35 B                    1                    1                    0\n" +
                                            "  ~p99                   35 B                    1                    1                    0\n" +
                                            "  ~p999                  35 B                    1                    1                    0\n" +
                                            "  min                    33 B                    1                    1                    0\n" +
                                            "  max                    35 B                    1                    1                    0\n" +
                                            "  count                     5\n";
    private static final String SUMMARY_2 = "               Partition size            Row count           Cell count      Tombstone count\n" +
                                            "  ~p50             71.735 KiB                   50                   50                    0\n" +
                                            "  ~p75             71.735 KiB                   50                   50                    0\n" +
                                            "  ~p90             71.735 KiB                   50                   50                    0\n" +
                                            "  ~p95             71.735 KiB                   50                   50                    0\n" +
                                            "  ~p99             71.735 KiB                   50                   50                    0\n" +
                                            "  ~p999            71.735 KiB                   50                   50                    0\n" +
                                            "  min              65.625 KiB                   50                   50                    0\n" +
                                            "  max              65.630 KiB                   50                   50                    0\n" +
                                            "  count                     5\n";

    @BeforeClass
    public static void prepareDirectories()
    {
        createBackupsAndSnapshots(SSTABLE_1);
        createBackupsAndSnapshots(SSTABLE_2);
    }

    private static void createBackupsAndSnapshots(String sstable)
    {
        File parentDir = new File(sstable).parent();

        File backupsDir = new File(parentDir, Directories.BACKUPS_SUBDIR);
        backupsDir.tryCreateDirectory();

        File snapshotsDir = new File(parentDir, Directories.SNAPSHOT_SUBDIR);
        snapshotsDir.tryCreateDirectory();

        File snapshotDir = new File(snapshotsDir, "snapshot-1");
        snapshotDir.tryCreateDirectory();

        for (File f : parentDir.tryList(File::isFile))
        {
            FileUtils.copyWithOutConfirm(f, new File(backupsDir, f.name()));
            FileUtils.copyWithOutConfirm(f, new File(snapshotDir, f.name()));
        }
    }

    /**
     * Runs post-test assertions about loaded classed and started threads.
     */
    @After
    public void assertPostTestEnv()
    {
        assertNoUnexpectedThreadsStarted(OPTIONAL_THREADS_WITH_SCHEMA, false);
        assertCLSMNotLoaded();
        assertSystemKSNotLoaded();
        assertKeyspaceNotLoaded();
        assertServerNotLoaded();
    }

    /**
     * Verify that the tool prints help when no arguments are provided.
     */
    @Test
    public void testNoArgsPrintsHelp()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTablePartitions.class);
        Assertions.assertThat(tool.getExitCode()).isOne();
        Assertions.assertThat(tool.getCleanedStderr()).contains("You must supply at least one sstable or directory");
        Assertions.assertThat(tool.getStdout()).contains("usage");
    }

    /**
     * Verify that the tool prints the right help contents.
     * If you added, modified options or help, please update docs if necessary.
     */
    @Test
    public void testMaybeChangeDocs()
    {
        ToolResult tool = ToolRunner.invokeClass(SSTablePartitions.class);
        Assertions.assertThat(tool.getStdout())
                  .isEqualTo("usage: sstablepartitions <options> <sstable files or directories>\n" +
                             "Print partition statistics of one or more sstables.\n" +
                             " -b,--backups                   include backups present in data\n" +
                             "                                directories (recursive scans)\n" +
                             " -c,--min-cells <arg>           partition cell count threshold\n" +
                             " -k,--key <arg>                 Partition keys to include\n" +
                             " -m,--csv                       CSV output (machine readable)\n" +
                             " -o,--min-tombstones <arg>      partition tombstone count threshold\n" +
                             " -r,--recursive                 scan for sstables recursively\n" +
                             " -s,--snapshots                 include snapshots present in data\n" +
                             "                                directories (recursive scans)\n" +
                             " -t,--min-size <arg>            partition size threshold, expressed as\n" +
                             "                                either the number of bytes or a size with\n" +
                             "                                unit of the form 10KiB, 20MiB, 30GiB, etc.\n" +
                             " -u,--current-timestamp <arg>   timestamp (seconds since epoch, unit time)\n" +
                             "                                for TTL expired calculation\n" +
                             " -w,--min-rows <arg>            partition row count threshold\n" +
                             " -x,--exclude-key <arg>         Excluded partition key(s) from partition\n" +
                             "                                detailed row/cell/tombstone information\n" +
                             "                                (irrelevant, if --partitions-only is\n" +
                             "                                given)\n" +
                             " -y,--partitions-only           Do not process per-partition detailed\n" +
                             "                                row/cell/tombstone information, only brief\n" +
                             "                                information\n");
    }

    /**
     * Verify that the tool can select single sstable file.
     */
    @Test
    public void testSingleSSTable()
    {
        assertThatToolSucceds(SSTABLE_1).isEqualTo(HEADER_1 + SUMMARY_1);
        assertThatToolSucceds(SSTABLE_2).isEqualTo(HEADER_2 + SUMMARY_2);
    }

    /**
     * Verify that the tool can select multiple sstable files.
     */
    @Test
    public void testMultipleSSTables()
    {
        assertThatToolSucceds(SSTABLE_1, SSTABLE_2)
                .isEqualTo(HEADER_2 + SUMMARY_2 + HEADER_1 + SUMMARY_1);
    }

    /**
     * Verify that the tool can select all the sstable files in a directory.
     */
    @Test
    public void testDirectory()
    {
        assertThatToolSucceds(new File(SSTABLE_1).parentPath())
                .isEqualTo(HEADER_1 + SUMMARY_1);

        assertThatToolSucceds("-r", new File(SSTABLE_1).parent().parentPath())
                .contains(HEADER_1 + SUMMARY_1)
                .contains(HEADER_2 + SUMMARY_2);

        assertThatToolSucceds("--recursive", new File(SSTABLE_2).parent().parentPath())
                .contains(HEADER_1 + SUMMARY_1)
                .contains(HEADER_2 + SUMMARY_2);
    }

    /**
     * Test the flag for collecting and printing sstable partition sizes only.
     */
    @Test
    public void testPartitionsOnly()
    {
        testPartitionsOnly("-y");
        testPartitionsOnly("--partitions-only");
    }

    private static void testPartitionsOnly(String option)
    {
        assertThatToolSucceds(SSTABLE_1, option)
                .isEqualTo(HEADER_1 +
                           "               Partition size\n" +
                           "  ~p50                   35 B\n" +
                           "  ~p75                   35 B\n" +
                           "  ~p90                   35 B\n" +
                           "  ~p95                   35 B\n" +
                           "  ~p99                   35 B\n" +
                           "  ~p999                  35 B\n" +
                           "  min                    33 B\n" +
                           "  max                    35 B\n" +
                           "  count                     5\n");

        assertThatToolSucceds(SSTABLE_2, "--partitions-only")
                .isEqualTo(HEADER_2 +
                           "               Partition size\n" +
                           "  ~p50             71.735 KiB\n" +
                           "  ~p75             71.735 KiB\n" +
                           "  ~p90             71.735 KiB\n" +
                           "  ~p95             71.735 KiB\n" +
                           "  ~p99             71.735 KiB\n" +
                           "  ~p999            71.735 KiB\n" +
                           "  min              65.625 KiB\n" +
                           "  max              65.630 KiB\n" +
                           "  count                     5\n");
    }

    /**
     * Test the flag for detecting partitions over a certain size threshold.
     */
    @Test
    public void testMinSize()
    {
        testMinSize("-t");
        testMinSize("--min-size");
    }

    private static void testMinSize(String option)
    {
        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "35")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 +
                           HEADER_1 +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  4 partitions match\n" +
                           "  Keys: 1 2 3 4\n" +
                           SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, "--min-size", "36")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, "--min-size", "67201")
                .isEqualTo(HEADER_2 +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  4 partitions match\n" +
                           "  Keys: 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, "--min-size", "67201B")
                .isEqualTo(HEADER_2 +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  4 partitions match\n" +
                           "  Keys: 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, "--min-size", "65KiB")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);
    }

    /**
     * Test the flag for detecting partitions with more cells than a certain threshold.
     */
    @Test
    public void testMinCells()
    {
        testMinCells("-c");
        testMinCells("--min-cells");
    }

    private static void testMinCells(String option)
    {
        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "0")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 +
                           HEADER_1 +
                           "  Partition: '0' (30) live, size: 33 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "2")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "51")
                .isEqualTo(HEADER_2 + SUMMARY_2 +
                           HEADER_1 + SUMMARY_1);
    }

    /**
     * Test the flag for detecting partitions with more rows than a certain threshold.
     */
    @Test
    public void testMinRows()
    {
        testMinRows("-w");
        testMinRows("--min-rows");
    }

    private static void testMinRows(String option)
    {
        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "0")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 +
                           HEADER_1 +
                           "  Partition: '0' (30) live, size: 33 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "50")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 + HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "51")
                .isEqualTo(HEADER_2 + SUMMARY_2 +
                           HEADER_1 + SUMMARY_1);
    }

    /**
     * Test the flag for detecting partitions with more tombstones than a certain threshold.
     */
    @Test
    public void testMinTombstones()
    {
        testMinTombstones("-o");
        testMinTombstones("--min-tombstones");
    }

    private static void testMinTombstones(String option)
    {
        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, option, "0")
                .isEqualTo(HEADER_2 +
                           "  Partition: '0' (30) live, size: 65.625 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 65.630 KiB, rows: 50, cells: 50, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_2 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_2 +
                           HEADER_1 +
                           "  Partition: '0' (30) live, size: 33 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  5 partitions match\n" +
                           "  Keys: 0 1 2 3 4\n" +
                           SUMMARY_1);

        assertThatToolSucceds(SSTABLE_1, SSTABLE_2, "--min-tombstones", "1")
                .isEqualTo(HEADER_2 + SUMMARY_2 + HEADER_1 + SUMMARY_1);
    }

    /**
     * Test the flag for providing a current time.
     */
    @Test
    public void testCurrentTimestamp()
    {
        testCurrentTimestamp("-u");
        testCurrentTimestamp("--current-timestamp");
    }

    private static void testCurrentTimestamp(String option)
    {
        String now = String.valueOf(FBUtilities.nowInSeconds());
        assertThatToolSucceds(SSTABLE_1, option, now).isEqualTo(HEADER_1 + SUMMARY_1);
    }

    /**
     * Test the flag for including backup sstables.
     */
    @Test
    public void testBackups()
    {
        testBackups("-b");
        testBackups("--backups");
    }

    private static void testBackups(String option)
    {
        assertThatToolSucceds(new File(SSTABLE_1).parentPath(), "-r", option)
                .isEqualTo(HEADER_1 + SUMMARY_1 + BACKUPS_HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(new File(SSTABLE_2).parentPath(), "-r", option)
                .isEqualTo(HEADER_2 + SUMMARY_2 + BACKUPS_HEADER_2 + SUMMARY_2);
    }

    /**
     * Test the flag for including snapshot sstables.
     */
    @Test
    public void testSnapshots()
    {
        testSnapshots("-s");
        testSnapshots("--snapshots");
    }

    private static void testSnapshots(String option)
    {
        assertThatToolSucceds(new File(SSTABLE_1).parentPath(), "-r", option)
                .isEqualTo(HEADER_1 + SUMMARY_1 + SNAPSHOTS_HEADER_1 + SUMMARY_1);

        assertThatToolSucceds(new File(SSTABLE_2).parentPath(), "-r", option)
                .isEqualTo(HEADER_2 + SUMMARY_2 + SNAPSHOTS_HEADER_2 + SUMMARY_2);
    }

    /**
     * Test the flag for specifying partiton keys to be considered.
     */
    @Test
    public void testIncludedKeys()
    {
        testIncludedKeys("-k");
        testIncludedKeys("--key");
    }

    private static void testIncludedKeys(String option)
    {
        assertThatToolSucceds(SSTABLE_1, "--min-size", "0", option, "1", option, "3")
                .contains(HEADER_1 +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  2 partitions match\n" +
                           "  Keys: 1 3\n")
                .contains("count                     2\n");

        assertThatToolSucceds(SSTABLE_1, "--min-size", "0", option, "0", option, "2", option, "4")
                .contains(HEADER_1 +
                          "  Partition: '0' (30) live, size: 33 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                          "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                          "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                          "Summary of  #1 (big-ma):\n" +
                          "  File: " + SSTABLE_1 + "\n" +
                          "  3 partitions match\n" +
                          "  Keys: 0 2 4\n")
                .contains("count                     3\n");

        assertThatToolSucceds(SSTABLE_1, "-y","--min-size", "0", option, "0", option, "2", option, "4")
                .contains(HEADER_1 +
                          "  Partition: '0' (30) live, size: 33 B\n" +
                          "  Partition: '2' (32) live, size: 35 B\n" +
                          "  Partition: '4' (34) live, size: 35 B\n" +
                          "Summary of  #1 (big-ma):\n" +
                          "  File: " + SSTABLE_1 + "\n" +
                          "  3 partitions match\n" +
                          "  Keys: 0 2 4\n")
                .contains("count                     3\n");
    }

    /**
     * Test the flag for specifying partiton keys to be excluded.
     */
    @Test
    public void testExcludedKeys()
    {
        testExcludedKeys("-x");
        testExcludedKeys("--exclude-key");
    }

    private static void testExcludedKeys(String option)
    {
        assertThatToolSucceds(SSTABLE_1, "--min-size", "0", option, "1", option, "3")
                .contains(HEADER_1 +
                           "  Partition: '0' (30) live, size: 33 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '2' (32) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '4' (34) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  3 partitions match\n" +
                           "  Keys: 0 2 4\n")
                .contains("count                     3\n");

        assertThatToolSucceds(SSTABLE_1, "--min-size", "0", option, "0", option, "2", option, "4")
                .contains(HEADER_1 +
                           "  Partition: '1' (31) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "  Partition: '3' (33) live, size: 35 B, rows: 1, cells: 1, tombstones: 0 (row:0, range:0, complex:0, cell:0, row-TTLd:0, cell-TTLd:0)\n" +
                           "Summary of  #1 (big-ma):\n" +
                           "  File: " + SSTABLE_1 + "\n" +
                           "  2 partitions match\n" +
                           "  Keys: 1 3\n")
                .contains("count                     2\n");
    }

    /**
     * Test the flag for producing machine-readable CSV output.
     */
    @Test
    public void testCSV()
    {
        testCSV("-m");
        testCSV("--csv");
    }

    private static void testCSV(String option)
    {
        assertThatToolSucceds(option, "--min-size", "35", SSTABLE_1, SSTABLE_2)
                .isEqualTo(format("key,keyBinary,live,offset,size,rowCount,cellCount,tombstoneCount," +
                                  "rowTombstoneCount,rangeTombstoneCount,complexTombstoneCount,cellTombstoneCount," +
                                  "rowTtlExpired,cellTtlExpired,directory,keyspace,table,index,snapshot,backup," +
                                  "generation,format,version\n" +
                                  "\"0\",30,true,0,67200,50,50,0,0,0,0,0,0,0,%s,,,,,,1,big,ma\n" +
                                  "\"1\",31,true,67200,67205,50,50,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"2\",32,true,134405,67205,50,50,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"3\",33,true,201610,67205,50,50,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"4\",34,true,268815,67205,50,50,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"1\",31,true,33,35,1,1,0,0,0,0,0,0,0,%s,,,,,,1,big,ma\n" +
                                  "\"2\",32,true,68,35,1,1,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"3\",33,true,103,35,1,1,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n" +
                                  "\"4\",34,true,138,35,1,1,0,0,0,0,0,0,0,%<s,,,,,,1,big,ma\n",
                                  SSTABLE_2, SSTABLE_1));
    }

    private static AbstractStringAssert<?> assertThatToolSucceds(String... args)
    {
        ToolResult tool = invokeTool(args);
        Assertions.assertThat(tool.getExitCode()).isZero();
        tool.assertOnCleanExit();
        return Assertions.assertThat(tool.getStdout());
    }

    private static String sstable(String table)
    {
        try
        {
            return findOneSSTable("legacy_sstables", table);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static ToolResult invokeTool(String... args)
    {
        return ToolRunner.invokeClass(SSTablePartitions.class, args);
    }
}
