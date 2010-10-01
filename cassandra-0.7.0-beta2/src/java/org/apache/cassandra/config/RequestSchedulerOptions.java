package org.apache.cassandra.config;

import java.util.Map;
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

/**
 *
 */
public class RequestSchedulerOptions
{
    public static final Integer DEFAULT_THROTTLE_LIMIT = 80;
    public static final Integer DEFAULT_WEIGHT = 1;

    public Integer throttle_limit = DEFAULT_THROTTLE_LIMIT;
    public Integer default_weight = DEFAULT_WEIGHT;
    public Map<String, Integer> weights;
}
