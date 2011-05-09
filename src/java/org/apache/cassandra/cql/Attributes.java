/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.cql;

import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * Class to contain attributes for statements
 */
public class Attributes
{
    private ConsistencyLevel cLevel;
    private Long timestamp;
    private int timeToLive;

    public Attributes()
    {}

    public Attributes(ConsistencyLevel cLevel, Long timestamp, int timeToLive)
    {
        this.cLevel = cLevel;
        this.timestamp = timestamp;
        this.timeToLive = timeToLive;
    }

    public ConsistencyLevel getConsistencyLevel()
    {
        return cLevel;
    }

    public void setConsistencyLevel(ConsistencyLevel cLevel)
    {
        this.cLevel = cLevel;
    }

    public Long getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(Long timestamp)
    {
        this.timestamp = timestamp;
    }

    public int getTimeToLive()
    {
        return timeToLive;
    }

    public void setTimeToLive(int timeToLive)
    {
        this.timeToLive = timeToLive;
    }

    public String toString()
    {
        return String.format("Attributes(consistency=%s, timestamp=%s, timeToLive=%s)", cLevel, timestamp, timeToLive);
    }

}
