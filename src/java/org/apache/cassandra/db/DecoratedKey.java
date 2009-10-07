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

package org.apache.cassandra.db;

import org.apache.cassandra.dht.Token;

/**
 * Represents a decorated key, handy for certain operations
 * where just working with strings gets slow.
 */
public class DecoratedKey<T extends Token> implements Comparable<DecoratedKey>
{
    public final T token;
    public final String key;

    public DecoratedKey(T token, String key)
    {
        super();
        assert key != null;
        this.token = token;
        this.key = key;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((token == null) ? 0 : token.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        DecoratedKey other = (DecoratedKey) obj;
        // either both should be of a class where all tokens are null, or neither
        assert (token == null) == (other.token == null);
        if (token == null)
            return key.equals(other.key);
        return token.equals(other.token) && key.equals(other.key);
    }

    public int compareTo(DecoratedKey other)
    {
        assert (token == null) == (other.token == null);
        if (token == null)
            return key.compareTo(other.key);
        int i = token.compareTo(other.token);
        return i == 0 ? key.compareTo(other.key) : i;
    }

    @Override
    public String toString()
    {
        return "DecoratedKey(" + token + ", " + key + ")";
    }
}
