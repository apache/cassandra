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
package org.apache.cassandra.tools;

import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Map;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;

public class RepairRunner extends AbstractFuture<Boolean> implements Runnable, NotificationListener
{
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    private final PrintStream out;
    private final StorageServiceMBean ssProxy;
    private final String keyspace;
    private final Map<String, String> options;

    private volatile int cmd;
    private volatile boolean success;

    public RepairRunner(PrintStream out, StorageServiceMBean ssProxy, String keyspace, Map<String, String> options)
    {
        this.out = out;
        this.ssProxy = ssProxy;
        this.keyspace = keyspace;
        this.options = options;
    }

    public void run()
    {
        cmd = ssProxy.repairAsync(keyspace, options);
        if (cmd <= 0)
        {
            String message = String.format("[%s] Nothing to repair for keyspace '%s'", format.format(System.currentTimeMillis()), keyspace);
            out.println(message);
            set(true);
        }
    }

    public void handleNotification(Notification notification, Object handback)
    {
        if ("repair".equals(notification.getType()))
        {
            int[] status = (int[]) notification.getUserData();
            assert status.length == 2;
            if (cmd == status[0])
            {
                String message = String.format("[%s] %s", format.format(notification.getTimeStamp()), notification.getMessage());
                out.println(message);
                // repair status is int array with [0] = cmd number, [1] = status
                if (status[1] == ActiveRepairService.Status.SESSION_FAILED.ordinal())
                {
                    success = false;
                }
                else if (status[1] == ActiveRepairService.Status.FINISHED.ordinal())
                {
                    set(success);
                }
            }
        }
        else if (JMXConnectionNotification.NOTIFS_LOST.equals(notification.getType()))
        {
            String message = String.format("[%s] Lost notification. You should check server log for repair status of keyspace %s",
                                           format.format(notification.getTimeStamp()),
                                           keyspace);
            out.println(message);
        }
        else if (JMXConnectionNotification.FAILED.equals(notification.getType())
                 || JMXConnectionNotification.CLOSED.equals(notification.getType()))
        {
            String message = String.format("JMX connection closed. You should check server log for repair status of keyspace %s"
                                           + "(Subsequent keyspaces are not going to be repaired).",
                                           keyspace);
            setException(new IOException(message));
        }
    }
}
