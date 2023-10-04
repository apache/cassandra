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
package org.apache.cassandra.journal;

public interface Params
{
    enum FlushMode { BATCH, GROUP, PERIODIC }

    enum FailurePolicy { STOP, STOP_JOURNAL, IGNORE, DIE }

    /**
     * @return maximum segment size
     */
    int segmentSize();

    /**
     * @return this journal's {@link FailurePolicy}
     */
    FailurePolicy failurePolicy();

    /**
     * @return journal flush (sync) mode
     */
    FlushMode flushMode();

    /**
     * @return milliseconds between journal flushes
     */
    int flushPeriodMillis();

    /**
     * @return milliseconds to block writes for while waiting for a slow disk flush to complete
     *         when in {@link FlushMode#PERIODIC} mode
     */
    int periodicFlushLagBlock();

    /**
     * @return user provided version to use for key and value serialization
     */
    int userVersion();
}
