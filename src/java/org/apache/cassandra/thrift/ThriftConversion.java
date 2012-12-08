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
package org.apache.cassandra.thrift;

import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;

/**
 * Static utility methods to convert internal structure to and from thrift ones.
 */
public class ThriftConversion
{
    public static ConsistencyLevel toThrift(org.apache.cassandra.db.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return ConsistencyLevel.ANY;
            case ONE: return ConsistencyLevel.ONE;
            case TWO: return ConsistencyLevel.TWO;
            case THREE: return ConsistencyLevel.THREE;
            case QUORUM: return ConsistencyLevel.QUORUM;
            case ALL: return ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return ConsistencyLevel.EACH_QUORUM;
        }
        throw new AssertionError();
    }

    public static org.apache.cassandra.db.ConsistencyLevel fromThrift(ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ANY: return org.apache.cassandra.db.ConsistencyLevel.ANY;
            case ONE: return org.apache.cassandra.db.ConsistencyLevel.ONE;
            case TWO: return org.apache.cassandra.db.ConsistencyLevel.TWO;
            case THREE: return org.apache.cassandra.db.ConsistencyLevel.THREE;
            case QUORUM: return org.apache.cassandra.db.ConsistencyLevel.QUORUM;
            case ALL: return org.apache.cassandra.db.ConsistencyLevel.ALL;
            case LOCAL_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.LOCAL_QUORUM;
            case EACH_QUORUM: return org.apache.cassandra.db.ConsistencyLevel.EACH_QUORUM;
        }
        throw new AssertionError();
    }

    public static void rethrow(RequestExecutionException e) throws UnavailableException, TimedOutException
    {
        if (e instanceof RequestTimeoutException)
            throw toThrift((RequestTimeoutException)e);
        else
            throw new UnavailableException();
    }

    public static InvalidRequestException toThrift(RequestValidationException e)
    {
        return new InvalidRequestException(e.getMessage());
    }

    public static InvalidRequestException toThrift(org.apache.cassandra.exceptions.InvalidRequestException e)
    {
        return new InvalidRequestException(e.getMessage());
    }

    public static UnavailableException toThrift(org.apache.cassandra.exceptions.UnavailableException e)
    {
        return new UnavailableException();
    }

    public static AuthenticationException toThrift(org.apache.cassandra.exceptions.AuthenticationException e)
    {
        return new AuthenticationException(e.getMessage());
    }

    public static TimedOutException toThrift(RequestTimeoutException e)
    {
        TimedOutException toe = new TimedOutException();
        if (e instanceof WriteTimeoutException)
        {
            WriteTimeoutException wte = (WriteTimeoutException)e;
            toe.setAcknowledged_by(wte.received);
            if (wte.writeType == WriteType.BATCH_LOG)
                toe.setAcknowledged_by_batchlog(false);
            else if (wte.writeType == WriteType.BATCH)
                toe.setAcknowledged_by_batchlog(true);
        }
        return toe;
    }
}
