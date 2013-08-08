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
package org.apache.cassandra.streaming.management;

import java.util.concurrent.atomic.AtomicLong;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;

import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamManagerMBean;
import org.apache.cassandra.streaming.StreamState;

/**
 */
public class StreamEventJMXNotifier extends NotificationBroadcasterSupport implements StreamEventHandler
{
    private final AtomicLong seq = new AtomicLong();

    public void handleStreamEvent(StreamEvent event)
    {
        Notification notif = null;
        switch (event.eventType) {
            case STREAM_PREPARED:
                notif = new Notification(StreamEvent.SessionPreparedEvent.class.getCanonicalName(),
                                                StreamManagerMBean.OBJECT_NAME,
                                                seq.getAndIncrement());
                notif.setUserData(SessionInfoCompositeData.toCompositeData(event.planId, ((StreamEvent.SessionPreparedEvent) event).session));
                break;
            case STREAM_COMPLETE:
                notif = new Notification(StreamEvent.SessionCompleteEvent.class.getCanonicalName(),
                                                StreamManagerMBean.OBJECT_NAME,
                                                seq.getAndIncrement());
                notif.setUserData(SessionCompleteEventCompositeData.toCompositeData((StreamEvent.SessionCompleteEvent) event));
                break;
            case FILE_PROGRESS:
                notif = new Notification(StreamEvent.ProgressEvent.class.getCanonicalName(),
                                         StreamManagerMBean.OBJECT_NAME,
                                         seq.getAndIncrement());
                notif.setUserData(ProgressInfoCompositeData.toCompositeData(event.planId, ((StreamEvent.ProgressEvent) event).progress));
                break;
        }
        sendNotification(notif);
    }

    public void onSuccess(StreamState result)
    {
        Notification notif = new Notification(StreamEvent.class.getCanonicalName() + ".success",
                                              StreamManagerMBean.OBJECT_NAME,
                                              seq.getAndIncrement());
        notif.setUserData(StreamStateCompositeData.toCompositeData(result));
        sendNotification(notif);
    }

    public void onFailure(Throwable t)
    {
        Notification notif = new Notification(StreamEvent.class.getCanonicalName() + ".failure",
                                              StreamManagerMBean.OBJECT_NAME,
                                              seq.getAndIncrement());
        notif.setUserData(t.fillInStackTrace().toString());
        sendNotification(notif);
    }
}
