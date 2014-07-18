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

/**
 * This interface defines an entity that can have permissions
 * granted to it.
 *
 */
public interface IGrantee extends Comparable<IGrantee>
{
    /**
     * The types of grantees currently supported
     *
     */
    enum Type
    {
        User,
        Role
    }
    /**
     * Return the type of the grantee
     *
     * @return the type of the grantee
     */
    public Type getType();

    /**
     * Return the name of the grantee
     *
     * @return the name of the grantee
     */
    public String getName();

    /**
     * Tests if the auth sub-system is aware of the grantee
     *
     * @return true if the grantee exists in the auth sub-system
     */
    public boolean isExisting();
}
