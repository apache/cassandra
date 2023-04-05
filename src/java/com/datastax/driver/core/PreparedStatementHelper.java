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

package com.datastax.driver.core;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// Unfortunately, MD5Digest and fields in PreparedStatement are package-private, so the easiest way to test these
// things while still using the driver was to create a class in DS package.
public class PreparedStatementHelper
{
    private static final MessageDigest cachedDigest;

    static {
        try {
            cachedDigest = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static MD5Digest id(PreparedStatement statement)
    {
        return statement.getPreparedId().boundValuesMetadata.id;
    }

    public static void assertStable(PreparedStatement first, PreparedStatement subsequent)
    {
        if (!id(first).equals(id(subsequent)))
        {
            throw new AssertionError(String.format("Subsequent id (%s) is different from the first one (%s)",
                                                   id(first),
                                                   id(subsequent)));
        }
    }

    public static void assertHashWithoutKeyspace(PreparedStatement statement, String queryString, String ks)
    {
        MD5Digest returned = id(statement);
        if (!returned.equals(hashWithoutKeyspace(queryString, ks)))
        {
            if (returned.equals(hashWithKeyspace(queryString, ks)))
                throw new AssertionError(String.format("Got hash with keyspace from the cluster: %s, should have gotten %s",
                                                       returned, hashWithoutKeyspace(queryString, ks)));
            else
                throw new AssertionError(String.format("Got unrecognized hash: %s",
                                                       returned));
        }
    }

    public static void assertHashWithKeyspace(PreparedStatement statement, String queryString, String ks)
    {
        MD5Digest returned = id(statement);
        if (!returned.equals(hashWithKeyspace(queryString, ks)))
        {
            if (returned.equals(hashWithoutKeyspace(queryString, ks)))
                throw new AssertionError(String.format("Got hash without keyspace from the cluster: %s, should have gotten %s",
                                                       returned, hashWithKeyspace(queryString, ks)));
            else
                throw new AssertionError(String.format("Got unrecognized hash: %s",
                                                       returned));
        }

    }

    public static boolean equalsToHashWithKeyspace(byte[] digest, String queryString, String ks)
    {
        return MD5Digest.wrap(digest).equals(hashWithKeyspace(queryString, ks));
    }

    public static MD5Digest hashWithKeyspace(String queryString, String ks)
    {
        return computeId(queryString, ks);
    }

    public static boolean equalsToHashWithoutKeyspace(byte[] digest, String queryString, String ks)
    {
        return MD5Digest.wrap(digest).equals(hashWithoutKeyspace(queryString, ks));
    }

    public static MD5Digest hashWithoutKeyspace(String queryString, String ks)
    {
        return computeId(queryString, null);
    }

    private static MD5Digest computeId(String queryString, String keyspace) {
        return compute(keyspace == null ? queryString : keyspace + queryString);
    }

    public static MD5Digest compute(String toHash) {
        try {
            return compute(toHash.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static synchronized MD5Digest compute(byte[] toHash) {
        cachedDigest.reset();
        return MD5Digest.wrap(cachedDigest.digest(toHash));
    }
}
