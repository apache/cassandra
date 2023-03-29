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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;

import org.apache.cassandra.db.Mutation;

public interface CommitLogReadHandler
{
    enum CommitLogReadErrorReason
    {
        RECOVERABLE_DESCRIPTOR_ERROR,
        UNRECOVERABLE_DESCRIPTOR_ERROR,
        MUTATION_ERROR,
        UNRECOVERABLE_UNKNOWN_ERROR,
        EOF
    }

    class CommitLogReadException extends IOException
    {
        public final CommitLogReadErrorReason reason;
        public final boolean permissible;

        CommitLogReadException(String message, CommitLogReadErrorReason reason, boolean permissible)
        {
            super(message);
            this.reason = reason;
            this.permissible = permissible;
        }
    }

    /**
     * Handle an error during segment read, signaling whether or not you want the reader to skip the remainder of the
     * current segment on error.
     * 在段读取过程中处理错误，指示您是否希望读取器在出现错误时跳过当前段的剩余部分。
     *
     * @param exception CommitLogReadException w/details on exception state
     * @return boolean indicating whether to stop reading
     * @throws IOException In the event the handler wants forceful termination of all processing, throw IOException.
     */
    boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException;

    /**
     * In instances where we cannot recover from a specific error and don't care what the reader thinks
     * 在我们无法从特定错误中恢复并且不在乎读者怎么想的情况下
     *
     * @param exception CommitLogReadException w/details on exception state
     * @throws IOException
     */
    void handleUnrecoverableError(CommitLogReadException exception) throws IOException;

    /**
     * Process a deserialized mutation
     * 处理反序列化的改变
     *
     * @param m deserialized mutation 反序列化突变
     * @param size serialized size of the mutation 改变的序列化大小
     * @param entryLocation filePointer offset inside the CommitLogSegment for the end of the   recordfileCommitLogSegment内记录结尾的指针偏移量
     * @param desc CommitLogDescriptor for mutation being processed 正在处理的变异的CommitLogDescriptor
     */
    void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc);
}
