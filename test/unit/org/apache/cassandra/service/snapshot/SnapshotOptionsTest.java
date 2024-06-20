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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class SnapshotOptionsTest
{
    @Test
    public void testSnapshotName()
    {
        List<SnapshotType> sameNameTypes = List.of(SnapshotType.DIAGNOSTICS, SnapshotType.REPAIR, SnapshotType.USER);

        for (SnapshotType type : sameNameTypes)
        {
            SnapshotOptions options = SnapshotOptions.systemSnapshot("a_name", type, "ks.tb")
                                                     .rateLimiter(RateLimiter.create(5))
                                                     .build();

            String snapshotName = options.getSnapshotName(Instant.now());
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

            String snapshotName = options.getSnapshotName(now);

            assertEquals(format("%d-%s-%s", now.toEpochMilli(), type.label, "a_name"), snapshotName);
        }
    }
}
