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
package org.apache.cassandra.db.monitoring;

/**
 * Base class for different types of bad queries. For every bad query at-least keyspace and
 * table information is mandatory to ease debugging.
 * Additionally each specific implementation can provide more details to make debugging easy.
 */
public abstract class BadQueryTypes
{
    String keySpace;
    String tableName;

    BadQueryTypes(String keySpace,
                  String tableName)
    {
        this.keySpace = keySpace;
        this.tableName = tableName;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ks:");
        sb.append(keySpace);
        sb.append(", table:");
        sb.append(tableName);
        return sb.toString();
    }

    //Useful to cleanup cached values periodically
    public void cleanup()
    {
    }

    abstract public String getKey();
    abstract public String getDetails();
}

