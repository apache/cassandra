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

package org.apache.cassandra.service.epaxos;

import java.io.PrintStream;
import java.util.concurrent.locks.Condition;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnectionNotification;

import org.apache.cassandra.utils.concurrent.SimpleCondition;

public interface UpgradeServiceMBean extends NotificationEmitter
{

    static final String COMPLETE_NOTIFICATION = UpgradeServiceMBean.class.getName() + ".COMPLETE";

    void upgradeNode();

    public boolean isUpgraded();

    public static class UpgradeRunner implements NotificationListener
    {
        private final PrintStream out;
        private final Condition condition = new SimpleCondition();

        public UpgradeRunner(PrintStream out)
        {
            this.out = out;
        }

        @Override
        public void handleNotification(Notification notification, Object handback)
        {
            if (JMXConnectionNotification.NOTIFS_LOST.equals(notification.getType()))
            {
                out.println("Lost notification. You should check server log for upgrade status");
                condition.signalAll();
            }
            else if (JMXConnectionNotification.FAILED.equals(notification.getType())
                     || JMXConnectionNotification.CLOSED.equals(notification.getType()))
            {
                out.println("JMX connection closed. You should check server log for upgrade status");
                condition.signalAll();
            }
            else if (COMPLETE_NOTIFICATION.equals(notification.getType()))
            {
                condition.signalAll();
            }
            else
            {
                out.println(String.format("[%s] %s", notification.getType(), notification.getMessage()));
            }
        }

        public void await()
        {
            try
            {
                condition.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
