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

package org.apache.cassandra.cdc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.ConfigurationException;

public class CDCSimpleLogPrinter implements ICDCHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CDCSimpleLogPrinter.class);

    @Override
    public void initialize(Map<String, String> options) throws ConfigurationException
    {
        logger.info("initialize handler with options: " + Arrays.toString(options.entrySet().toArray()));
    }

    @Override
    public void process(Mutation mutation) throws IOException
    {
        logger.info(mutation.toString());
    }
}
