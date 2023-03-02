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

import java.io.Closeable;
import java.util.Collection;
import java.util.function.Function;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

public interface IVerifier extends Closeable
{
    static Options.Builder options()
    {
        return new Options.Builder();
    }

    void verify();

    @Override
    void close();

    CompactionInfo.Holder getVerifyInfo();

    class Options
    {
        public final boolean invokeDiskFailurePolicy;

        /**
         * Force extended verification - unless it is enabled, extended verificiation will be done only
         * if there is no digest present. Setting it along with quick makes no sense.
         */
        public final boolean extendedVerification;

        public final boolean checkVersion;
        public final boolean mutateRepairStatus;
        public final boolean checkOwnsTokens;

        /**
         * Quick check which does not include sstable data verification.
         */
        public final boolean quick;

        public final Function<String, ? extends Collection<Range<Token>>> tokenLookup;

        private Options(boolean invokeDiskFailurePolicy,
                        boolean extendedVerification,
                        boolean checkVersion,
                        boolean mutateRepairStatus,
                        boolean checkOwnsTokens,
                        boolean quick,
                        Function<String, ? extends Collection<Range<Token>>> tokenLookup)
        {
            this.invokeDiskFailurePolicy = invokeDiskFailurePolicy;
            this.extendedVerification = extendedVerification;
            this.checkVersion = checkVersion;
            this.mutateRepairStatus = mutateRepairStatus;
            this.checkOwnsTokens = checkOwnsTokens;
            this.quick = quick;
            this.tokenLookup = tokenLookup;
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "invokeDiskFailurePolicy=" + invokeDiskFailurePolicy +
                   ", extendedVerification=" + extendedVerification +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   ", checkOwnsTokens=" + checkOwnsTokens +
                   ", quick=" + quick +
                   '}';
        }

        public static class Builder
        {
            private boolean invokeDiskFailurePolicy = false; // invoking disk failure policy can stop the node if we find a corrupt stable
            private boolean extendedVerification = false;
            private boolean checkVersion = false;
            private boolean mutateRepairStatus = false; // mutating repair status can be dangerous
            private boolean checkOwnsTokens = false;
            private boolean quick = false;
            private Function<String, ? extends Collection<Range<Token>>> tokenLookup = StorageService.instance::getLocalAndPendingRanges;

            public Builder invokeDiskFailurePolicy(boolean param)
            {
                this.invokeDiskFailurePolicy = param;
                return this;
            }

            public Builder extendedVerification(boolean param)
            {
                this.extendedVerification = param;
                return this;
            }

            public Builder checkVersion(boolean param)
            {
                this.checkVersion = param;
                return this;
            }

            public Builder mutateRepairStatus(boolean param)
            {
                this.mutateRepairStatus = param;
                return this;
            }

            public Builder checkOwnsTokens(boolean param)
            {
                this.checkOwnsTokens = param;
                return this;
            }

            public Builder quick(boolean param)
            {
                this.quick = param;
                return this;
            }

            public Builder tokenLookup(Function<String, ? extends Collection<Range<Token>>> tokenLookup)
            {
                this.tokenLookup = tokenLookup;
                return this;
            }

            public Options build()
            {
                return new Options(invokeDiskFailurePolicy, extendedVerification, checkVersion, mutateRepairStatus, checkOwnsTokens, quick, tokenLookup);
            }
        }
    }
}