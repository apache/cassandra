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

import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.notifications.INotification;

/**
 * Notification triggered by a {@link SSTablesGlobalTracker} when the set of sstables versions in use on this node
 * changes.
 *
 * <p>The notification includes the set of sstable versions in use when the notification is triggered (so the result
 * of the change triggering that notification).
 */
public class SSTablesVersionsInUseChangeNotification implements INotification
{
    /**
     * The set of all sstable versions in use on this node at the time of this notification.
     */
    public final ImmutableSet<Version> versionsInUse;

    SSTablesVersionsInUseChangeNotification(ImmutableSet<Version> versionsInUse)
    {
        this.versionsInUse = versionsInUse;
    }

    @Override
    public String toString()
    {
        return String.format("SSTablesInUseChangeNotification(%s)", versionsInUse.stream().map(Version::toFormatAndVersionString).collect(Collectors.toList()));
    }
}
