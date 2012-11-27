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
package org.apache.cassandra.db.columniterator;
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


import com.google.common.collect.AbstractIterator;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.OnDiskAtom;

import java.io.IOException;


/*
 * The goal of this encapsulating OnDiskAtomIterator is to delay the use of
 * the filter until columns are actually queried.
 * The reason for that is get_paged_slice because it change the start of
 * the filter after having seen the first row, and so we must not use the
 * filter before the row data is actually queried. However, mergeIterator
 * needs to "fetch" a row in advance. But all it needs is the key and so
 * this IColumnIterator make sure getKey() can be called without triggering
 * the use of the filter itself.
 */
public class LazyColumnIterator extends AbstractIterator<OnDiskAtom> implements OnDiskAtomIterator
{
    private final DecoratedKey key;
    private final IColumnIteratorFactory subIteratorFactory;

    private OnDiskAtomIterator subIterator;

    public LazyColumnIterator(DecoratedKey key, IColumnIteratorFactory subIteratorFactory)
    {
        this.key = key;
        this.subIteratorFactory = subIteratorFactory;
    }

    private OnDiskAtomIterator getSubIterator()
    {
        if (subIterator == null)
            subIterator = subIteratorFactory.create();
        return subIterator;
    }

    protected OnDiskAtom computeNext()
    {
        getSubIterator();
        return subIterator.hasNext() ? subIterator.next() : endOfData();
    }

    public ColumnFamily getColumnFamily()
    {
        return getSubIterator().getColumnFamily();
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public void close() throws IOException
    {
        if (subIterator != null)
            subIterator.close();
    }
}
