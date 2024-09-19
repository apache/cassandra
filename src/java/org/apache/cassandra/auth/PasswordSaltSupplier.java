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

package org.apache.cassandra.auth;

import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.mindrot.jbcrypt.BCrypt;

import static org.apache.cassandra.config.CassandraRelevantProperties.AUTH_BCRYPT_GENSALT_LOG2_ROUNDS;

public class PasswordSaltSupplier
{
    // 2 ** GENSALT_LOG2_ROUNDS rounds of hashing will be performed.
    private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();

    @VisibleForTesting
    static int getGensaltLogRounds()
    {
        int rounds = AUTH_BCRYPT_GENSALT_LOG2_ROUNDS.getInt();
        if (rounds < 4 || rounds > 30)
            throw new ConfigurationException(String.format("Bad value for system property -D%s." +
                                                           "Please use a value between 4 and 30 inclusively",
                                                           AUTH_BCRYPT_GENSALT_LOG2_ROUNDS.getKey()));
        return rounds;
    }
    private static Supplier<String> DEFAULT_SALT_SUPPLIER = () -> BCrypt.gensalt(GENSALT_LOG2_ROUNDS);
    private static Supplier<String> saltSupplier = DEFAULT_SALT_SUPPLIER;

    public static void unsafeSet(Supplier<String> newSaltSupplier)
    {
        assert newSaltSupplier != null;
        saltSupplier = newSaltSupplier;
    }
    public static void unsafeReset()
    {
        saltSupplier = DEFAULT_SALT_SUPPLIER;
    }

    public static String get()
    {
        return saltSupplier.get();
    }
}
