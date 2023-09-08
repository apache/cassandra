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

package org.apache.cassandra.distributed.util;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.distributed.api.IInvokableInstance;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class Auth
{

    public static void waitForExistingRoles(IInvokableInstance instance)
    {
        assert instance != null && !instance.isShutdown() && instance.isValid();

        await().pollDelay(1, SECONDS)
               .pollInterval(1, SECONDS)
               .atMost(60, SECONDS)
               .until(() -> instance.callOnInstance(CassandraRoleManager::hasExistingRoles));
    }

}
