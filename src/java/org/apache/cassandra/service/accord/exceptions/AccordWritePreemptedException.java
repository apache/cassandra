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

package org.apache.cassandra.service.accord.exceptions;

import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteTimeoutException;

import static org.apache.cassandra.db.ConsistencyLevel.SERIAL;

// quick hack to allow tests to tell the difference between preemption and other protocol timeouts
public class AccordWritePreemptedException extends WriteTimeoutException
{
    public AccordWritePreemptedException(int received, int blockFor)
    {
        super(WriteType.CAS, SERIAL, received, blockFor);
    }

    public AccordWritePreemptedException(int received, int blockFor, String msg)
    {
        super(WriteType.CAS, SERIAL, received, blockFor, msg);
    }
}
