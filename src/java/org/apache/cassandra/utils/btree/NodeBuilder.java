/*
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
 */
package org.apache.cassandra.utils.btree;

import org.apache.cassandra.utils.ObjectSizes;

import java.util.Arrays;
import java.util.Comparator;

import static org.apache.cassandra.utils.btree.BTree.EMPTY_BRANCH;
import static org.apache.cassandra.utils.btree.BTree.FAN_FACTOR;
import static org.apache.cassandra.utils.btree.BTree.compare;
import static org.apache.cassandra.utils.btree.BTree.find;
import static org.apache.cassandra.utils.btree.BTree.getKeyEnd;
import static org.apache.cassandra.utils.btree.BTree.isLeaf;

/**
 * Represents a level / stack item of in progress modifications to a BTree.
 */
final class NodeBuilder
{
    private static final int MAX_KEYS = 1 + (FAN_FACTOR * 2);

    // parent stack
    private NodeBuilder parent, child;

    // buffer for building new nodes
    private Object[] buildKeys = new Object[MAX_KEYS];  // buffers keys for branches and leaves
    private Object[] buildChildren = new Object[1 + MAX_KEYS]; // buffers children for branches only
    private int buildKeyPosition;
    private int buildChildPosition;
    // we null out the contents of buildKeys/buildChildren when clear()ing them for re-use; this is where
    // we track how much we actually have to null out
    private int maxBuildKeyPosition;

    // current node of the btree we're modifying/copying from
    private Object[] copyFrom;
    // the index of the first key in copyFrom that has not yet been copied into the build arrays
    private int copyFromKeyPosition;
    // the index of the first child node in copyFrom that has not yet been copied into the build arrays
    private int copyFromChildPosition;

    private UpdateFunction updateFunction;
    private Comparator comparator;

    // upper bound of range owned by this level; lets us know if we need to ascend back up the tree
    // for the next key we update when bsearch gives an insertion point past the end of the values
    // in the current node
    private Object upperBound;

    // ensure we aren't referencing any garbage
    void clear()
    {
        NodeBuilder current = this;
        while (current != null && current.upperBound != null)
        {
            current.clearSelf();
            current = current.child;
        }
        current = parent;
        while (current != null && current.upperBound != null)
        {
            current.clearSelf();
            current = current.parent;
        }
    }

    void clearSelf()
    {
        reset(null, null, null, null);
        Arrays.fill(buildKeys, 0, maxBuildKeyPosition, null);
        Arrays.fill(buildChildren, 0, maxBuildKeyPosition + 1, null);
        maxBuildKeyPosition = 0;
    }

    // reset counters/setup to copy from provided node
    void reset(Object[] copyFrom, Object upperBound, UpdateFunction updateFunction, Comparator comparator)
    {
        this.copyFrom = copyFrom;
        this.upperBound = upperBound;
        this.updateFunction = updateFunction;
        this.comparator = comparator;
        maxBuildKeyPosition = Math.max(maxBuildKeyPosition, buildKeyPosition);
        buildKeyPosition = 0;
        buildChildPosition = 0;
        copyFromKeyPosition = 0;
        copyFromChildPosition = 0;
    }

    NodeBuilder finish()
    {
        assert copyFrom != null;
        int copyFromKeyEnd = getKeyEnd(copyFrom);

        if (buildKeyPosition + buildChildPosition > 0)
        {
            // only want to copy if we've already changed something, otherwise we'll return the original
            copyKeys(copyFromKeyEnd);
            if (!isLeaf(copyFrom))
                copyChildren(copyFromKeyEnd + 1);
        }
        return isRoot() ? null : ascend();
    }

    /**
     * Inserts or replaces the provided key, copying all not-yet-visited keys prior to it into our buffer.
     *
     * @param key key we are inserting/replacing
     * @return the NodeBuilder to retry the update against (a child if we own the range being updated,
     * a parent if we do not -- we got here from an earlier key -- and we need to ascend back up),
     * or null if we finished the update in this node.
     */
    NodeBuilder update(Object key)
    {
        assert copyFrom != null;
        int copyFromKeyEnd = getKeyEnd(copyFrom);

        int i = copyFromKeyPosition;
        boolean found; // exact key match?
        boolean owns = true; // true if this node (or a child) should contain the key
        if (i == copyFromKeyEnd)
        {
            found = false;
        }
        else
        {
            // this optimisation is for the common scenario of updating an existing row with the same columns/keys
            // and simply avoids performing a binary search until we've checked the proceeding key;
            // possibly we should disable this check if we determine that it fails more than a handful of times
            // during any given builder use to get the best of both worlds
            int c = -comparator.compare(key, copyFrom[i]);
            if (c >= 0)
            {
                found = c == 0;
            }
            else
            {
                i = find(comparator, key, copyFrom, i + 1, copyFromKeyEnd);
                found = i >= 0;
                if (!found)
                    i = -i - 1;
            }
        }

        if (found)
        {
            Object prev = copyFrom[i];
            Object next = updateFunction.apply(prev, key);
            // we aren't actually replacing anything, so leave our state intact and continue
            if (prev == next)
                return null;
            key = next;
        }
        else if (i == copyFromKeyEnd && compare(comparator, key, upperBound) >= 0)
            owns = false;

        if (isLeaf(copyFrom))
        {

            if (owns)
            {
                // copy keys from the original node up to prior to the found index
                copyKeys(i);

                if (found)
                {
                    // if found, we've applied updateFunction already
                    replaceNextKey(key);
                }
                else
                {
                    // if not found, we still need to apply the update function
                    key = updateFunction.apply(key);
                    addNewKey(key); // handles splitting parent if necessary via ensureRoom
                }

                // done, so return null
                return null;
            }
            else
            {
                // we don't want to copy anything if we're ascending and haven't copied anything previously,
                // as in this case we can return the original node. Leaving buildKeyPosition as 0 indicates
                // to buildFromRange that it should return the original instead of building a new node
                if (buildKeyPosition > 0)
                    copyKeys(i);
            }

            // if we don't own it, all we need to do is ensure we've copied everything in this node
            // (which we have done, since not owning means pos >= keyEnd), ascend, and let Modifier.update
            // retry against the parent node.  The if/ascend after the else branch takes care of that.
        }
        else
        {
            // branch
            if (found)
            {
                copyKeys(i);
                replaceNextKey(key);
                copyChildren(i + 1);
                return null;
            }
            else if (owns)
            {
                copyKeys(i);
                copyChildren(i);

                // belongs to the range owned by this node, but not equal to any key in the node
                // so descend into the owning child
                Object newUpperBound = i < copyFromKeyEnd ? copyFrom[i] : upperBound;
                Object[] descendInto = (Object[]) copyFrom[copyFromKeyEnd + i];
                ensureChild().reset(descendInto, newUpperBound, updateFunction, comparator);
                return child;
            }
            else if (buildKeyPosition > 0 || buildChildPosition > 0)
            {
                // ensure we've copied all keys and children, but only if we've already copied something.
                // otherwise we want to return the original node
                copyKeys(copyFromKeyEnd);
                copyChildren(copyFromKeyEnd + 1); // since we know that there are exactly 1 more child nodes, than keys
            }
        }

        return ascend();
    }


    // UTILITY METHODS FOR IMPLEMENTATION OF UPDATE/BUILD/DELETE

    boolean isRoot()
    {
        // if parent == null, or parent.upperBound == null, then we have not initialised a parent builder,
        // so we are the top level builder holding modifications; if we have more than FAN_FACTOR items, though,
        // we are not a valid root so we would need to spill-up to create a new root
        return (parent == null || parent.upperBound == null) && buildKeyPosition <= FAN_FACTOR;
    }

    // ascend to the root node, splitting into proper node sizes as we go; useful for building
    // where we work only on the newest child node, which may construct many spill-over parents as it goes
    NodeBuilder ascendToRoot()
    {
        NodeBuilder current = this;
        while (!current.isRoot())
            current = current.ascend();
        return current;
    }

    // builds a new root BTree node - must be called on root of operation
    Object[] toNode()
    {
        assert buildKeyPosition <= FAN_FACTOR && (buildKeyPosition > 0 || copyFrom.length > 0) : buildKeyPosition;
        return buildFromRange(0, buildKeyPosition, isLeaf(copyFrom), false);
    }

    // finish up this level and pass any constructed children up to our parent, ensuring a parent exists
    private NodeBuilder ascend()
    {
        ensureParent();
        boolean isLeaf = isLeaf(copyFrom);
        if (buildKeyPosition > FAN_FACTOR)
        {
            // split current node and move the midpoint into parent, with the two halves as children
            int mid = buildKeyPosition / 2;
            parent.addExtraChild(buildFromRange(0, mid, isLeaf, true), buildKeys[mid]);
            parent.finishChild(buildFromRange(mid + 1, buildKeyPosition - (mid + 1), isLeaf, false));
        }
        else
        {
            parent.finishChild(buildFromRange(0, buildKeyPosition, isLeaf, false));
        }
        return parent;
    }

    // copy keys from copyf to the builder, up to the provided index in copyf (exclusive)
    private void copyKeys(int upToKeyPosition)
    {
        if (copyFromKeyPosition >= upToKeyPosition)
            return;

        int len = upToKeyPosition - copyFromKeyPosition;
        assert len <= FAN_FACTOR : upToKeyPosition + "," + copyFromKeyPosition;

        ensureRoom(buildKeyPosition + len);
        if (len > 0)
        {
            System.arraycopy(copyFrom, copyFromKeyPosition, buildKeys, buildKeyPosition, len);
            copyFromKeyPosition = upToKeyPosition;
            buildKeyPosition += len;
        }
    }

    // skips the next key in copyf, and puts the provided key in the builder instead
    private void replaceNextKey(Object with)
    {
        // (this first part differs from addNewKey in that we pass the replaced object to replaceF as well)
        ensureRoom(buildKeyPosition + 1);
        buildKeys[buildKeyPosition++] = with;

        copyFromKeyPosition++;
    }

    // puts the provided key in the builder, with no impact on treatment of data from copyf
    void addNewKey(Object key)
    {
        ensureRoom(buildKeyPosition + 1);
        buildKeys[buildKeyPosition++] = key;
    }

    // copies children from copyf to the builder, up to the provided index in copyf (exclusive)
    private void copyChildren(int upToChildPosition)
    {
        // (ensureRoom isn't called here, as we should always be at/behind key additions)
        if (copyFromChildPosition >= upToChildPosition)
            return;
        int len = upToChildPosition - copyFromChildPosition;
        if (len > 0)
        {
            System.arraycopy(copyFrom, getKeyEnd(copyFrom) + copyFromChildPosition, buildChildren, buildChildPosition, len);
            copyFromChildPosition = upToChildPosition;
            buildChildPosition += len;
        }
    }

    // adds a new and unexpected child to the builder - called by children that overflow
    private void addExtraChild(Object[] child, Object upperBound)
    {
        ensureRoom(buildKeyPosition + 1);
        buildKeys[buildKeyPosition++] = upperBound;
        buildChildren[buildChildPosition++] = child;
    }

    // adds a replacement expected child to the builder - called by children prior to ascending
    private void finishChild(Object[] child)
    {
        buildChildren[buildChildPosition++] = child;
        copyFromChildPosition++;
    }

    // checks if we can add the requested keys+children to the builder, and if not we spill-over into our parent
    private void ensureRoom(int nextBuildKeyPosition)
    {
        if (nextBuildKeyPosition < MAX_KEYS)
            return;

        // flush even number of items so we don't waste leaf space repeatedly
        Object[] flushUp = buildFromRange(0, FAN_FACTOR, isLeaf(copyFrom), true);
        ensureParent().addExtraChild(flushUp, buildKeys[FAN_FACTOR]);
        int size = FAN_FACTOR + 1;
        assert size <= buildKeyPosition : buildKeyPosition + "," + nextBuildKeyPosition;
        System.arraycopy(buildKeys, size, buildKeys, 0, buildKeyPosition - size);
        buildKeyPosition -= size;
        maxBuildKeyPosition = buildKeys.length;
        if (buildChildPosition > 0)
        {
            System.arraycopy(buildChildren, size, buildChildren, 0, buildChildPosition - size);
            buildChildPosition -= size;
        }
    }

    // builds and returns a node from the buffered objects in the given range
    private Object[] buildFromRange(int offset, int keyLength, boolean isLeaf, boolean isExtra)
    {
        // if keyLength is 0, we didn't copy anything from the original, which means we didn't
        // modify any of the range owned by it, so can simply return it as is
        if (keyLength == 0)
            return copyFrom;

        Object[] a;
        if (isLeaf)
        {
            a = new Object[keyLength + (keyLength & 1)];
            System.arraycopy(buildKeys, offset, a, 0, keyLength);
        }
        else
        {
            a = new Object[1 + (keyLength * 2)];
            System.arraycopy(buildKeys, offset, a, 0, keyLength);
            System.arraycopy(buildChildren, offset, a, keyLength, keyLength + 1);
        }
        if (isExtra)
            updateFunction.allocated(ObjectSizes.sizeOfArray(a));
        else if (a.length != copyFrom.length)
            updateFunction.allocated(ObjectSizes.sizeOfArray(a) -
                                     (copyFrom.length == 0 ? 0 : ObjectSizes.sizeOfArray(copyFrom)));
        return a;
    }

    // checks if there is an initialised parent, and if not creates/initialises one and returns it.
    // different to ensureChild, as we initialise here instead of caller, as parents in general should
    // already be initialised, and only aren't in the case where we are overflowing the original root node
    private NodeBuilder ensureParent()
    {
        if (parent == null)
        {
            parent = new NodeBuilder();
            parent.child = this;
        }
        if (parent.upperBound == null)
            parent.reset(EMPTY_BRANCH, upperBound, updateFunction, comparator);
        return parent;
    }

    // ensures a child level exists and returns it
    NodeBuilder ensureChild()
    {
        if (child == null)
        {
            child = new NodeBuilder();
            child.parent = this;
        }
        return child;
    }
}
