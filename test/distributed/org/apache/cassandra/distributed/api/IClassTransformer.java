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
package org.apache.cassandra.distributed.api;

public interface IClassTransformer {

    /**
     * Modify the bytecode of the provided class. Provides the original bytecode and the fully qualified name of the class.
     * Note, bytecode may be null indicating the class definition could not be found. In this case a synthetic definition
     * may be returned, or null.
     */
    byte[] transform(String name, byte[] bytecode);

    default IClassTransformer initialise() { return this; }
}
