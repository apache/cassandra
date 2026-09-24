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
 * Review probe (audit Gap 7): runs the early-open boundary scenario under BTI while KEEPING
 * {@code requiresMidStreamReopen() == true}. The parent's sibling {@code BtiCursorEarlyOpenBoundaryTest}
 * overrides that gate to false, so it never asserts a mid-stream reopen actually fired under BTI.
 *
 * If this test passes, BTI does publish an OpenReason.EARLY reader mid-compaction on the cursor path
 * with the inherited fixture sizing, and the sibling's under-assertion is unnecessary. If it fails on
 * the midStream assertion, BTI has the code path but never fires it mid-stream at this fixture size.
 */
public class BtiCursorEarlyOpenMidStreamProbeTest extends CursorEarlyOpenBoundaryTest
{
    @Override
    protected String formatName()
    {
        return "bti";
    }

    // Deliberately NOT overriding requiresMidStreamReopen(): it stays true, so the midStream > 0
    // assertion in assertBoundaryIsDetached is enforced under BTI.
}
