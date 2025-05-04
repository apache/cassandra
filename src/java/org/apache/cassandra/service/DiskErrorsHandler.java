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

package org.apache.cassandra.service;

import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

public interface DiskErrorsHandler extends AutoCloseable
{
    void init();

    void handleCorruptSSTable(CorruptSSTableException e);

    void handleFSError(FSError e);

    boolean handleCommitError(String message, Throwable t);

    void handleStartupFSError(Throwable t);

    void inspectDiskError(Throwable t);

    void inspectCommitLogError(Throwable t);

    class NoOpDiskErrorHandler implements DiskErrorsHandler
    {
        public static final DiskErrorsHandler NO_OP = new NoOpDiskErrorHandler();

        private NoOpDiskErrorHandler() {}

        @Override
        public void inspectCommitLogError(Throwable t) {}

        @Override
        public boolean handleCommitError(String message, Throwable t)
        {
            // tracks what DefaultDiskErrorsHandler does when commit_failure_policy = ignore
            return true;
        }

        @Override
        public void handleCorruptSSTable(CorruptSSTableException e) {}

        @Override
        public void handleFSError(FSError e) {}

        @Override
        public void handleStartupFSError(Throwable t) {}

        @Override
        public void inspectDiskError(Throwable t) {}

        @Override
        public void init() {}

        @Override
        public void close() throws Exception {}
    }
}
