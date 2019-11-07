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

package org.apache.cassandra.repair;

public interface Progress
{
    /**
     * What time this object was created.
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    long getCreationtTimeNs();

    /**
     * The time the entity started; this is when the actual work is running, so is different than {@link #getCreationtTimeNs()}
     * which reflects when the work was created (so can be used to track queue time).
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    long getStartTimeNs();

    String getFailureCause();

    /**
     * The time the last mutation was made.
     *
     * Uses {@link System#nanoTime()} so does not reflect unix epoch.
     */
    long getLastUpdatedAtNs();

    boolean isComplete();

    /**
     * @return 0.0 to 1.0 to represent estimate on validation progress
     */
    float getProgress();
}
