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

package org.apache.cassandra.service.snapshot;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;

import com.google.common.util.concurrent.RateLimiter;

import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;

import static java.lang.String.format;
import static org.apache.cassandra.service.snapshot.SnapshotType.USER;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class SnapshotOptionsTest
{
    @Test
    public void testSnapshotNameValidation()
    {
        String sep = File.pathSeparator();

        try (WithProperties p = new WithProperties().set(CassandraRelevantProperties.SNAPSHOT_NAME_VALIDATION, true))
        {
            // Only alphanumerics, '-', '_' and '.' are accepted.
            validate("atag", USER);
            validate("a-tag", USER);
            validate("a_tag", USER);
            validate("a_tag" + Instant.now().toEpochMilli(), USER);
            validate("a_tag_1and_something2-more", USER);
            validate("a".repeat(255), USER);
            validate("snap.2026-05-20", USER);
            // Dots embedded in a name are not traversal: with '/' excluded, "a..tag" is just a literal directory.
            validate("a..tag", USER);

            assertThatThrownBy(() -> validate("a".repeat(256), USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name must not be more than 255 characters long for " +
                        "resolved snapshot name (got 256 characters for \"" + "a".repeat(256) + "\")");

            // this would append timestamp + type which would violate < 255 when tag is 250 chars long only
            assertThatThrownBy(() -> validate("a".repeat(250), SnapshotType.UPGRADE))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Snapshot name must not be more than 255 characters long");

            // '/' is not in the allowed set; this is what kills traversal attempts like "../../mysnapshot".
            assertThatThrownBy(() -> validate('a' + sep + "tag", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name cannot contain " + sep);

            // The shell-significant S3 "safe" characters (! * ' ( )) are deliberately NOT allowed.
            assertThatThrownBy(() -> validate("important!", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: important!");
            assertThatThrownBy(() -> validate("backup*", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: backup*");
            assertThatThrownBy(() -> validate("o'snap", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: o'snap");
            assertThatThrownBy(() -> validate("snap(1)", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: snap(1)");

            // Other characters outside the allowed set must still be rejected.
            assertThatThrownBy(() -> validate("a tag", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: a tag");
            assertThatThrownBy(() -> validate("a:tag", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name contains illegal characters: a:tag");

            // "." and ".." pass the charset check but resolve to the snapshots/ dir itself
            // and its parent (the live table dir) respectively, so they must be rejected as reserved.
            assertThatThrownBy(() -> validate(".", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '.' is reserved");

            assertThatThrownBy(() -> validate("..", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '..' is reserved");
        }

        try (WithProperties p = new WithProperties().set(CassandraRelevantProperties.SNAPSHOT_NAME_VALIDATION, false))
        {
            // The character check is bypassed entirely: space, ':', and the now-disallowed
            // shell-significant characters (! * ' ( )) are all accepted.
            assertThatCode(() -> validate("a tag", USER)).doesNotThrowAnyException();
            assertThatCode(() -> validate("a:tag", USER)).doesNotThrowAnyException();
            assertThatCode(() -> validate("important!", USER)).doesNotThrowAnyException();
            assertThatCode(() -> validate("snap(1)", USER)).doesNotThrowAnyException();

            assertThatCode(() -> validate("a".repeat(256), USER)).doesNotThrowAnyException();
            assertThatCode(() -> validate("a".repeat(250), SnapshotType.UPGRADE)).doesNotThrowAnyException();

            // Path separator and "." / ".." rejections are unconditional — they guard against
            // traversal regardless of the toggle.
            assertThatThrownBy(() -> validate('a' + sep + "tag", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name cannot contain " + sep);
            assertThatThrownBy(() -> validate(".", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '.' is reserved");
            assertThatThrownBy(() -> validate("..", USER))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Snapshot name '..' is reserved");
        }
    }

    private void validate(String tag, SnapshotType type)
    {
        new SnapshotOptions.Builder(tag, type, s -> true, "ks.tb").rateLimiter(RateLimiter.create(1)).build();
    }

    @Test
    public void testSnapshotName()
    {
        List<SnapshotType> sameNameTypes = List.of(SnapshotType.DIAGNOSTICS, SnapshotType.REPAIR, USER);

        for (SnapshotType type : sameNameTypes)
        {
            SnapshotOptions options = SnapshotOptions.systemSnapshot("a_name", type, "ks.tb")
                                                     .rateLimiter(RateLimiter.create(5))
                                                     .build();

            String snapshotName = SnapshotOptions.getSnapshotName(type, options.tag, Instant.now());
            assertEquals("a_name", snapshotName);
        }

        EnumSet<SnapshotType> snapshotTypes = EnumSet.allOf(SnapshotType.class);
        snapshotTypes.removeAll(sameNameTypes);

        for (SnapshotType type : snapshotTypes)
        {
            SnapshotOptions options = SnapshotOptions.systemSnapshot("a_name", type, "ks.tb")
                                                     .rateLimiter(RateLimiter.create(5))
                                                     .build();

            Instant now = Instant.now();

            String snapshotName = SnapshotOptions.getSnapshotName(options.type, options.tag, now);

            assertEquals(format("%d-%s-%s", now.toEpochMilli(), type.label, "a_name"), snapshotName);
        }
    }
}
