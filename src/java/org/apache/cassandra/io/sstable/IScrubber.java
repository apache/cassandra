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

package org.apache.cassandra.io.sstable;

import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.utils.Closeable;

public interface IScrubber extends Closeable
{
    void scrub();

    void close();

    CompactionInfo.Holder getScrubInfo();

    @VisibleForTesting
    ScrubResult scrubWithResult();

    static Options.Builder options()
    {
        return new Options.Builder();
    }

    final class ScrubResult
    {
        public final int goodPartitions;
        public final int badPartitions;
        public final int emptyPartitions;

        public ScrubResult(int goodPartitions, int badPartitions, int emptyPartitions)
        {
            this.goodPartitions = goodPartitions;
            this.badPartitions = badPartitions;
            this.emptyPartitions = emptyPartitions;
        }
    }

    class Options
    {
        public final boolean checkData;
        public final boolean reinsertOverflowedTTLRows;
        public final boolean skipCorrupted;

        private Options(boolean checkData, boolean reinsertOverflowedTTLRows, boolean skipCorrupted)
        {
            this.checkData = checkData;
            this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
            this.skipCorrupted = skipCorrupted;
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", Options.class.getSimpleName() + "[", "]")
                   .add("checkData=" + checkData)
                   .add("reinsertOverflowedTTLRows=" + reinsertOverflowedTTLRows)
                   .add("skipCorrupted=" + skipCorrupted)
                   .toString();
        }

        public static class Builder
        {
            private boolean checkData = false;
            private boolean reinsertOverflowedTTLRows = false;
            private boolean skipCorrupted = false;

            public Builder checkData()
            {
                this.checkData = true;
                return this;
            }

            public Builder checkData(boolean checkData)
            {
                this.checkData = checkData;
                return this;
            }

            public Builder reinsertOverflowedTTLRows()
            {
                this.reinsertOverflowedTTLRows = true;
                return this;
            }

            public Builder reinsertOverflowedTTLRows(boolean reinsertOverflowedTTLRows)
            {
                this.reinsertOverflowedTTLRows = reinsertOverflowedTTLRows;
                return this;
            }

            public Builder skipCorrupted()
            {
                this.skipCorrupted = true;
                return this;
            }

            public Builder skipCorrupted(boolean skipCorrupted)
            {
                this.skipCorrupted = skipCorrupted;
                return this;
            }

            public Options build()
            {
                return new Options(checkData, reinsertOverflowedTTLRows, skipCorrupted);
            }
        }
    }
}