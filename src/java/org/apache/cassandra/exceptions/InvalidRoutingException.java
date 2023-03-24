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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;

public class InvalidRoutingException extends InvalidRequestException
{
    public static final String TOKEN_TEMPLATE = "Received a read request from %s for a token %s that is not owned by the current replica as of %s: %s.";
    public static final String RANGE_TEMPLATE = "Received a read request from %s for a range [%s,%s] that is not owned by the current replica as of %s: %s.";
    private InvalidRoutingException(String msg)
    {
        super(msg);
    }

    public static InvalidRoutingException forTokenRead(InetAddressAndPort from,
                                                       Token token,
                                                       Epoch epoch,
                                                       ReadCommand command)
    {
        return new InvalidRoutingException(String.format(TOKEN_TEMPLATE, from, token, epoch, command));
    }

    public static InvalidRoutingException forRangeRead(InetAddressAndPort from,
                                                       AbstractBounds<?> range,
                                                       Epoch epoch,
                                                       ReadCommand command)
    {
        return new InvalidRoutingException(String.format(RANGE_TEMPLATE, from, range.left, range.right, epoch, command));
    }
}
