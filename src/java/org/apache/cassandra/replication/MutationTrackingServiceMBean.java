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

package org.apache.cassandra.replication;

/**
 * MBean exposing functionality for the Mutation Tracking service
 */
public interface MutationTrackingServiceMBean
{
    /**
     * Sets the background reconciliation state to enabled/disabled based on the {@param enabled} parameter
     *
     * @param enabled whether the background reconciliation is enabled or disabled for the mutation tracking service
     */
    void setMutationTrackingBackgroundReconciliationEnabled(boolean enabled);

    /**
     * @return the state of the background reconciliation for the mutation tracking service
     */
    boolean getMutationTrackingBackgroundReconciliationEnabled();

    /**
     * Sets the background reconciliation interval to the provided {@param intervalMilliseconds} value
     *
     * @param intervalMilliseconds the interval value in milliseconds
     */
    void setMutationTrackingBackgroundReconciliationIntervalMilliseconds(long intervalMilliseconds);

    /**
     * @return the interval, in milliseconds, in which the background reconciliation runs when enabled
     */
    long getMutationTrackingBackgroundReconciliationIntervalMilliseconds();
}
