/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.vector;

import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JVectorVersionUtil} to verify feature support (FusedPQ, NVQ)
 * is correctly determined based on jvector format version across all SAI versions.
 */
@RunWith(Parameterized.class)
public class JVectorVersionUtilTest extends SAITester
{
    private static final int JVECTOR_FORMAT_FUSED_PQ = 6;  // Format version that introduced FusedPQ
    private static final int JVECTOR_FORMAT_NVQ = 4;       // Format version that introduced NVQ

    @Parameterized.Parameter()
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        // Test all versions that support vector indexes
        return Version.ALL.stream()
                          .filter(v -> v.onOrAfter(Version.JVECTOR_EARLIEST))
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    @Test
    public void testFusedPQSupport()
    {
        // versionSupportsFused: true for FA and FB (jvector format 6+), false for pre-FA
        int jvectorFormat = version.onDiskFormat().jvectorFileFormatVersion();
        boolean expectedSupport = jvectorFormat >= JVECTOR_FORMAT_FUSED_PQ;

        assertEquals(String.format("Version %s (jvector format %d) FusedPQ support mismatch", version, jvectorFormat),
                     expectedSupport,
                     JVectorVersionUtil.versionSupportsFused(version));

        // shouldWriteFused: FA always on; FB+ requires ENABLE_FUSED flag (default false); pre-FA always false
        boolean expectedWrite = version.equals(Version.FA);  // default flag is false, so only FA is unconditionally on
        assertEquals(String.format("Version %s (jvector format %d) shouldWriteFused mismatch (ENABLE_FUSED=false)", version, jvectorFormat),
                     expectedWrite,
                     JVectorVersionUtil.shouldWriteFused(version));
    }

    @Test
    public void testNVQSupport()
    {
        // NVQ requires jvector format 4 or later
        int jvectorFormat = version.onDiskFormat().jvectorFileFormatVersion();
        boolean expectedSupport = jvectorFormat >= JVECTOR_FORMAT_NVQ;
        
        assertEquals(String.format("Version %s (jvector format %d) NVQ support mismatch", version, jvectorFormat),
                     expectedSupport,
                     JVectorVersionUtil.versionSupportsNVQ(version));
    }
}
