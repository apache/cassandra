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
package org.apache.cassandra.hints;

import java.util.List;
import java.util.Map;

public interface HintsServiceMBean
{
    /**
     * Pause dispatch of all hints. Does not affect the creation of hints.
     */
    void pauseDispatch();

    /**
     * Resume dispatch of all hints. Does not affect the creation of hints.
     */
    void resumeDispatch();

    /**
     * Irrevocably deletes all the stored hints files (with the exception of those that are being dispatched right now,
     * or being written to).
     */
    void deleteAllHints();

    /**
     * Irrevocably deletes all the stored hints files for the target address (with the exception of those that are
     * being dispatched right now, or being written to).
     */
    void deleteAllHintsForEndpoint(String address);

    /**
     * Returns all pending hints that this node has.
     *
     * @return a list of endpoints with relevant hint information - total number of files, newest and oldest timestamps.
     */
    List<Map<String, String>> getPendingHints();
}
