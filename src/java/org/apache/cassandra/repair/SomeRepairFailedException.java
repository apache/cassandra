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

/**
 * This is a special exception which states "I know something failed but I don't have access to the failure". This
 * is mostly used to make sure the error notifications are clean and the history table has a meaningful exception.
 *
 * The expected behavior is that when this is thrown, this error should be ignored from history table and not used
 * for notifications
 */
public class SomeRepairFailedException extends RuntimeException
{
    public static final SomeRepairFailedException INSTANCE = new SomeRepairFailedException();

    private SomeRepairFailedException()
    {
        super(null, null, false, false);
    }
}
