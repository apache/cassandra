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
package org.apache.cassandra.exceptions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;

public class WriteFailureException extends RequestFailureException
{
    public final WriteType writeType;

    public WriteFailureException(ConsistencyLevel consistency, int received, int failures, int blockFor, WriteType writeType)
    {
        super(ExceptionCode.WRITE_FAILURE, consistency, received, failures, blockFor);
        this.writeType = writeType;
    }
}
