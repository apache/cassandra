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
package org.apache.cassandra.db;

/**
 * This interface marks objects that are only valid in a restricted scope and
 * shouldn't be simply aliased outside of this scope (in other words, you should
 * not keep a reference to the object that escaped said scope as the object will
 * likely become invalid).
 *
 * For instance, most {@link RowIterator} implementation reuse the same {@link
 * Row} object during iteration. This means that the following code would be
 * incorrect.
 * <pre>
 *    RowIterator iter = ...;
 *    Row someRow = null;
 *    while (iter.hasNext())
 *    {
 *        Row row = iter.next();
 *        if (someCondition(row))
 *           someRow = row;         // This isn't safe
 *        doSomethingElse();
 *    }
 *    useRow(someRow);
 * </pre>
 * The problem being that, because the row iterator reuse the same object,
 * {@code someRow} will not point to the row that had met {@code someCondition}
 * at the end of the iteration ({@code someRow} will point to the last iterated
 * row in practice).
 *
 * When code do need to alias such {@code Aliasable} object, it should call the
 * {@code takeAlias} method that will make a copy of the object if necessary.
 *
 * Of course, the {@code takeAlias} should not be abused, as it defeat the purpose
 * of sharing objects in the first place.
 *
 * Also note that some implementation of an {@code Aliasable} object may be
 * safe to alias, in which case its {@code takeAlias} method will be a no-op.
 */
public interface Aliasable<T>
{
    /**
     * Returns either this object (if it's safe to alias) or a copy of it
     * (it it isn't safe to alias).
     */
    public T takeAlias();
}
