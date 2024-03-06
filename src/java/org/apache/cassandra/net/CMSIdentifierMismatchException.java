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

/**
 * Exception thrown in case of a CMS identifier mismatch. This should usually not happen, except rare cases of
 * network partition during CMS election during initial cluster bringup. This is just a precaution to avoid
 * corrupting CMS log.
 */
public class CMSIdentifierMismatchException extends RuntimeException
{
    public CMSIdentifierMismatchException(String format)
    {
        super(format);
    }
}
