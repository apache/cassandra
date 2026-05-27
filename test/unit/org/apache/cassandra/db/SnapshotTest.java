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

package org.apache.cassandra.db;

import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.SchemaConstants;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SnapshotTest extends CQLTester
{
    @Before
    public void setUpTable() throws Throwable
    {
        createTable("create table %s (id int primary key, k int)");
        execute("insert into %s (id, k) values (1,1)");
    }

    @Test
    public void testEmptyTOC() throws Throwable
    {
        getCurrentColumnFamilyStore().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        for (SSTableReader sstable : getCurrentColumnFamilyStore().getLiveSSTables())
        {
            File toc = new File(sstable.descriptor.filenameFor(Component.TOC));
            Files.write(toc.toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        }
        getCurrentColumnFamilyStore().snapshot("hello");
    }

    @Test
    public void testSnapshotNameValidation()
    {
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        String sep = File.pathSeparator();

        try (WithProperties p = new WithProperties())
        {
            p.set(CassandraRelevantProperties.SNAPSHOT_NAME_VALIDATION, true);

            // Previously-allowed alphanumerics, '-' and '_' must still be accepted.
            assertThatCode(() -> cfs.snapshot("atag")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("a-tag")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("a_tag")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("a_tag_1and_something2-more")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot(repeat('a', SchemaConstants.FILENAME_LENGTH))).doesNotThrowAnyException();

            // AWS S3 "Safe characters" accepted by the relaxed allowlist:
            //   !  .  *  '  (  )
            // See https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-guidelines
            assertThatCode(() -> cfs.snapshot("snap.2026-05-20")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("important!")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("backup*")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("o'snap")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("snap(1)")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("!._-*'()")).doesNotThrowAnyException();
            // Dots embedded in a name are not traversal: with '/' excluded, "a..tag" is just a literal directory.
            assertThatCode(() -> cfs.snapshot("a..tag")).doesNotThrowAnyException();

            String tooLong = repeat('a', SchemaConstants.FILENAME_LENGTH + 1);
            assertThatThrownBy(() -> cfs.snapshot(tooLong))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name must not be more than 255 characters long for " +
                        "resolved snapshot name (got 256 characters for \"" + tooLong + "\")");

            // '/' is not in the S3-safe set; this is what kills traversal attempts like "../../mysnapshot".
            assertThatThrownBy(() -> cfs.snapshot("a" + sep + "tag"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name cannot contain " + sep);

            // Other characters outside the S3-safe set must still be rejected.
            assertThatThrownBy(() -> cfs.snapshot("a tag"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: a tag");
            assertThatThrownBy(() -> cfs.snapshot("a:tag"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: a:tag");

            // "." and ".." pass the charset check but resolve to the snapshots/ dir itself
            // and its parent (the live table dir) respectively, so they must be rejected as reserved.
            assertThatThrownBy(() -> cfs.snapshot("."))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '.' is reserved");
            assertThatThrownBy(() -> cfs.snapshot(".."))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '..' is reserved");
        }

        try (WithProperties p = new WithProperties())
        {
            p.set(CassandraRelevantProperties.SNAPSHOT_NAME_VALIDATION, false);

            // Previously-rejected characters are now accepted: space, ':', and other non-S3-safe chars.
            assertThatCode(() -> cfs.snapshot("a tag")).doesNotThrowAnyException();
            assertThatCode(() -> cfs.snapshot("a:tag")).doesNotThrowAnyException();

            // Path separator and "." / ".." rejections are unconditional — they guard against
            // traversal regardless of the toggle.
            assertThatThrownBy(() -> cfs.snapshot("a" + sep + "tag"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name cannot contain " + sep);
            assertThatThrownBy(() -> cfs.snapshot("."))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '.' is reserved");
            assertThatThrownBy(() -> cfs.snapshot(".."))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '..' is reserved");
        }
    }

    private static String repeat(char c, int times)
    {
        char[] chars = new char[times];
        java.util.Arrays.fill(chars, c);
        return new String(chars);
    }
}
