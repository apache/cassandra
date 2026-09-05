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


/**
 * Runs the inherited allocation tests with the BTI format selected. The measured region then
 * covers BTI's index path: per-block boundary prefix copies, IndexInfo, and open-marker
 * snapshots.
 * <p>
 * The wide-schema sparse-row ceiling is deliberately NOT overridden. BTI measures 0.505-0.514
 * B/B there against BIG's 0.352-0.353, and the inherited 0.6 leaves the same headroom the
 * complex-column ceiling below keeps. An override would only loosen it.
 */
public class BtiCursorCompactionAllocationGateTest extends CursorCompactionAllocationGateTest
{
    @Override
    protected String formatName()
    {
        return "bti";
    }

    @Override
    protected long ceilingBytes()
    {
        return 768 * 1024;
    }

    /**
     * The same per-partition BTI index cost, expressed per input byte. Marker-dense partitions
     * are small, about 10KB, so 2KB per partition adds 0.2 to 0.3 B/B over the BIG residual.
     * Measured 1.012 B/B under BTI against 0.684 under BIG. A leak of one small object per
     * marker costs more than 1.5 B/B, so a ceiling of 1.3 still fails.
     */
    @Override
    protected double rtPerInputByteCeiling()
    {
        return 1.3;
    }

    /**
     * The complex-column test runs at multi-MB scale, unlike the range-tombstone one. BTI's
     * 2KB per partition then spreads across far more input bytes and barely moves the ratio.
     * Measured 0.511 B/B under BTI against about 0.5 under BIG, whose ceiling is 0.5. A
     * ceiling of 0.6 keeps headroom comparable to the other ceilings and still fails on a
     * per-row regression.
     */
    @Override
    protected double complexPerInputByteCeiling()
    {
        return 0.6;
    }

    /**
     * The large-file test compacts ~40MB, so BTI's per-partition index cost spreads thin: measured
     * 0.247-0.248 B/B over three runs, against 0.178-0.179 under BIG. The inherited 0.5 was
     * calibrated for BIG and leaves BTI room to double its allocation unnoticed. 0.32 keeps the
     * ~30% headroom the range-tombstone ceiling above uses.
     */
    @Override
    protected double largeFilePerInputByteCeiling()
    {
        return 0.32;
    }
}
