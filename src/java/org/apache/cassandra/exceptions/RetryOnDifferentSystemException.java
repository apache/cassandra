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

package org.apache.cassandra.exceptions;

/**
 * Thrown when a non-transactional operation is attempted when the operation needs to be done transactionally (or vice versa)
 * and it could interfere with operations performed transactionally or can't be applied by the chosen transaction system.
 *
 * The correct way to handle this is to forward the error the originator of the operation who can then retry it on
 * the correct system.
 */
public class RetryOnDifferentSystemException extends RuntimeException
{
}
