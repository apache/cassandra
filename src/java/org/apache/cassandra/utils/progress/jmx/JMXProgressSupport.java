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
package org.apache.cassandra.utils.progress.jmx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressListener;

import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

/**
 * ProgressListener that translates ProgressEvent to JMX Notification message.
 */
public class JMXProgressSupport implements ProgressListener
{
    private final AtomicLong notificationSerialNumber = new AtomicLong();

    private final NotificationBroadcasterSupport broadcaster;

    public JMXProgressSupport(NotificationBroadcasterSupport broadcaster)
    {
        this.broadcaster = broadcaster;
    }

    @Override
    public void progress(String tag, ProgressEvent event)
    {
        Notification notification = new Notification("progress",
                                                     tag,
                                                     notificationSerialNumber.getAndIncrement(),
                                                     currentTimeMillis(),
                                                     event.getMessage());
        Map<String, Integer> userData = new HashMap<>();
        userData.put("type", event.getType().ordinal());
        userData.put("progressCount", event.getProgressCount());
        userData.put("total", event.getTotal());
        notification.setUserData(userData);
        broadcaster.sendNotification(notification);
    }
}
