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
package org.apache.cassandra.hadoop.pig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @deprecated use CqlNativeStorage instead. CqlStorage will be removed.
 */
public class CqlStorage extends CqlNativeStorage
{
    private static final Logger logger = LoggerFactory.getLogger(CqlNativeStorage.class);

    public CqlStorage()
    {
        this(1000);
        logger.warn("CqlStorage is deprecated and will be removed in the next release, use CqlNativeStorage instead.");
    }

    /** @param pageSize limit number of CQL rows to fetch in a thrift request */
    public CqlStorage(int pageSize)
    {
        super(pageSize);
    }
}

