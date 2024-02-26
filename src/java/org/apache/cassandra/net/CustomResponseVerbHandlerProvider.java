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

package org.apache.cassandra.net;

import java.util.function.Supplier;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Priovides a response handler for response messages ({@link org.apache.cassandra.net.Verb#REQUEST_RSP} and
 * {@link org.apache.cassandra.net.Verb#FAILURE_RSP}).
 * Defaults to {@link ResponseVerbHandler#instance}.
 */
public interface CustomResponseVerbHandlerProvider extends Supplier<IVerbHandler<?>>
{
    CustomResponseVerbHandlerProvider instance = CassandraRelevantProperties.CUSTOM_RESPONSE_VERB_HANDLER_PROVIDER.getString() == null ?
                                                 () -> ResponseVerbHandler.instance :
                                                 FBUtilities.construct(CassandraRelevantProperties.CUSTOM_RESPONSE_VERB_HANDLER_PROVIDER.getString(), "custom response verb handler");

    IVerbHandler<?> get();

}
