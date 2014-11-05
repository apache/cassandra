/**
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
package org.apache.cassandra.dht;

import com.google.common.primitives.Longs;

public class LongToken extends Token
{
    static final long serialVersionUID = -5833580143318243006L;

    final long token;

    public LongToken(long token)
    {
        this.token = token;
    }

    public String toString()
    {
        return Long.toString(token);
    }

    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token == (((LongToken)obj).token);
    }

    public int hashCode()
    {
        return Longs.hashCode(token);
    }

    public int compareTo(Token o)
    {
        return Long.compare(token, ((LongToken) o).token);
    }

    public Long getTokenValue()
    {
        return token;
    }
}
