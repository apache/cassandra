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

package org.apache.cassandra.distributed.api;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.Notification;

import org.junit.Assert;

public class NodeToolResult
{
    private final String[] commandAndArgs;
    private final int rc;
    private final List<Notification> notifications;
    private final Throwable error;

    public NodeToolResult(String[] commandAndArgs, int rc, List<Notification> notifications, Throwable error)
    {
        this.commandAndArgs = commandAndArgs;
        this.rc = rc;
        this.notifications = notifications;
        this.error = error;
    }

    public String[] getCommandAndArgs()
    {
        return commandAndArgs;
    }

    public int getRc()
    {
        return rc;
    }

    public List<Notification> getNotifications()
    {
        return notifications;
    }

    public Throwable getError()
    {
        return error;
    }

    public Asserts asserts()
    {
        return new Asserts();
    }

    public final class Asserts {
        public Asserts success() {
            Assert.assertEquals("nodetool command " + commandAndArgs[0] + " was not successful", 0, rc);
            return this;
        }

        public Asserts failure() {
            Assert.assertNotEquals("nodetool command " + commandAndArgs[0] + " was successful but not expected to be", 0, rc);
            return this;
        }

        public Asserts errorContains(String msg) {
            Assert.assertNotNull("No exception was found but expected one", error);
            Assert.assertTrue("Error message '" + error.getMessage() + "' does not contain '" + msg + "'", error.getMessage().contains(msg));
            return this;
        }

        public Asserts notificationContains(String msg) {
            Assert.assertNotNull("notifications not defined", notifications);
            Assert.assertFalse("notifications not defined", notifications.isEmpty());
            for (Notification n : notifications) {
                if (n.getMessage().contains(msg)) {
                    return this;
                }
            }
            Assert.fail("Unable to locate message " + msg + " in notifications: " + notifications);
            return this; // unreachable
        }

        public Asserts notificationContains(ProgressEventType type, String msg) {
            int userType = type.ordinal();
            Assert.assertNotNull("notifications not defined", notifications);
            Assert.assertFalse("notifications not defined", notifications.isEmpty());
            for (Notification n : notifications) {
                if (notificationType(n) == userType) {
                    if (n.getMessage().contains(msg)) {
                        return this;
                    }
                }
            }
            Assert.fail("Unable to locate message '" + msg + "' in notifications: " + notifications);
            return this; // unreachable
        }
    }

    private static int notificationType(Notification n)
    {
        return ((Map<String, Integer>) n.getUserData()).get("type").intValue();
    }

    public String toString()
    {
        return "NodeToolResult{" +
               "commandAndArgs=" + Arrays.toString(commandAndArgs) +
               ", rc=" + rc +
               ", notifications=[" + notifications.stream().map(n -> ProgressEventType.values()[notificationType(n)].name()).collect(Collectors.joining(", ")) + "]" +
               ", error=" + error +
               '}';
    }

    /**
     * Progress event type.
     *
     * <p>
     * Progress starts by emitting {@link #START}, followed by emitting zero or more {@link #PROGRESS} events,
     * then it emits either one of {@link #ERROR}/{@link #ABORT}/{@link #SUCCESS}.
     * Progress indicates its completion by emitting {@link #COMPLETE} at the end of process.
     * </p>
     * <p>
     * {@link #NOTIFICATION} event type is used to just notify message without progress.
     * </p>
     */
    public enum ProgressEventType
    {
        /**
         * Fired first when progress starts.
         * Happens only once.
         */
        START,

        /**
         * Fire when progress happens.
         * This can be zero or more time after START.
         */
        PROGRESS,

        /**
         * When observing process completes with error, this is sent once before COMPLETE.
         */
        ERROR,

        /**
         * When observing process is aborted by user, this is sent once before COMPLETE.
         */
        ABORT,

        /**
         * When observing process completes successfully, this is sent once before COMPLETE.
         */
        SUCCESS,

        /**
         * Fire when progress complete.
         * This is fired once, after ERROR/ABORT/SUCCESS is fired.
         * After this, no more ProgressEvent should be fired for the same event.
         */
        COMPLETE,

        /**
         * Used when sending message without progress.
         */
        NOTIFICATION
    }
}
