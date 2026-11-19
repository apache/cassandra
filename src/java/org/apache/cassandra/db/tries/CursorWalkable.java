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
package org.apache.cassandra.db.tries;

/// Package-private interface for trie implementations, defining a method of extracting the internal cursor
/// representation of the trie.
///
/// @param <C> The specific type of cursor a descendant uses.
interface CursorWalkable<C extends Cursor>
{
    /// Returns a cursor that can be used to walk over the trie. The cursor will be positioned at the root of the trie
    /// and prepared to walk it in the given direction.
    C cursor(Direction direction);
}
