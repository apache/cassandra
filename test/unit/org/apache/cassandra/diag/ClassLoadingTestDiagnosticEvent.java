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

package org.apache.cassandra.diag;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

/**
 * A minimal, valid {@link DiagnosticEvent} subclass used by {@code DiagnosticEventPersistenceTest} to confirm that
 * the type-checked load path in {@link DiagnosticEventPersistence} resolves a correct subtype. Lives in the
 * {@code org.apache.cassandra} namespace so it passes the persistence package-prefix guard.
 */
public final class ClassLoadingTestDiagnosticEvent extends DiagnosticEvent
{
    public enum TestType { TEST }

    @Override
    public Enum<?> getType()
    {
        return TestType.TEST;
    }

    @Override
    public Map<String, Serializable> toMap()
    {
        return Collections.emptyMap();
    }
}
