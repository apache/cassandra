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

package org.apache.cassandra.distributed.test.auth;

import java.io.IOException;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class CIDRAuthorizerConfigTest extends TestBaseImpl
{
    @Test
    public void testParameterizedClass() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.set("cidr_authorizer", new ParameterizedClass("CassandraCIDRAuthorizer",
                                                                                                              ImmutableMap.of("cidr_authorizer_mode", "ENFORCE")))
                                                               .set("authorizer.class_name", "CassandraAuthorizer")
                                                               .set("authenticator.class_name", "PasswordAuthenticator"))
                                             .start()))
        {
            // just makes sure we can start with a param in the ParameterizedClass
        }
    }

    @Test
    public void testParameterizedClass_no_params() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(c -> c.set("cidr_authorizer.class_name","CassandraCIDRAuthorizer")
                                                               .set("authorizer.class_name", "CassandraAuthorizer")
                                                               .set("authenticator.class_name", "PasswordAuthenticator"))
                                             .start()))
        {
            // just makes sure we can start without a param in the ParameterizedClass
        }
    }
}
