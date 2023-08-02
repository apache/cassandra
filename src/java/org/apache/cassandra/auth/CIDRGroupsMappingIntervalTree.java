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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.cql3.CIDR;

/**
 * This class implements CIDR Interval tree and the ability to find longest matching CIDR for the given IP.
 * CIDRs interval tree is a variant of interval tree. Each node contains a CIDR and a value.
 * A node has left children array and the right children array.
 * - The left children's CIDRs are either less than the starting IP of parent or overlaps with the parent node.
 * - The right children's CIDRs are either greater than the ending IP of the parent or overlaps with the parent node.
 * Note that nodes that overlap with the parent node are included in both left and right children arrays.
 *
 * The tree organizes nodes by placing non-overlapping CIDRs at the same level. In general, CIDRs with the same net mask
 * do not overlap, hence are placed in the same level. CIDRs with different net mask may overlap, hence placed at
 * different levels in the tree. In addition to this, there is an optimisation to promote a CIDR to an upper level, if
 * it is not overlapping with any CIDR in the parent level, that means, in such cases a CIDR with different net mask can
 * co-locate in the same level with other CIDRs.
 *
 * Levels closer to the root contains CIDRs with higher net mask value. Net mask value decreases as levels further down
 * from the root. i.e, Nearer the level to the root, the narrower the CIDR, meaning matching the longer IP prefix.
 *
 * Search for Longest matching CIDR for an IP starts at level 0, if not found a match, search continues to the next
 * level, until it finds a match or reaches leaf nodes without a match. That means search terminates on the first match
 * closest to the root, i.e, locates narrowest matching CIDR.
 *
 * Example:
 * Assume below CIDRs
 * "128.10.120.2/10", ==> IP range 128.0.0.0 - 128.63.255.255, netmask 10
 * "128.20.120.2/20", ==> IP range 128.20.112.0 - 128.20.127.255, netmask 20
 * "0.0.0.0/0",       ==> IP range 0.0.0.0 - 255.255.255.255, netmask 0
 * "10.1.1.2/10"      ==> IP range 10.0.0.0 - 10.63.255.255, netmask 10
 *
 * Resulting interval tree looks like:
 *
 * (10.0.0.0 - 10.63.255.255, 10)  (128.20.112.0 - 128.20.127.255, 20)
 *               /             \               / \
 *              /              (128.0.0.0 - 128.63.255.255, 10)
 *             /                      /  \
 *            (0.0.0.0 - 255.255.255.255, 0)
 *
 * Note that in this example (10.0.0.0 - 10.63.255.255, 10) doesn't have any overlapping CIDR, hence moved up a level as
 * an optimization
 */
public class CIDRGroupsMappingIntervalTree<V> implements CIDRGroupsMappingTable<V>
{
    private final IPIntervalTree<V> tree;

    /**
     * Build an interval tree for given CIDRs
     * @param isIPv6 true if this CIDR groups mapping is for IPv6 IPs, false otherwise
     * @param cidrMappings CIDR to Value mappings
     */
    public CIDRGroupsMappingIntervalTree(boolean isIPv6, Map<CIDR, Set<V>> cidrMappings)
    {
        for (CIDR cidr : cidrMappings.keySet())
        {
            if (isIPv6 != cidr.isIPv6())
                throw new IllegalArgumentException("Invalid CIDR format, expecting " + getIPTypeString(isIPv6) +
                                                   ", received " + getIPTypeString(cidr.isIPv6()));
        }

        this.tree = IPIntervalTree.build(new ArrayList<>(cidrMappings
                                                         .entrySet()
                                                         .stream()
                                                         .collect(Collectors.groupingBy(p -> p.getKey().getNetMask(),
                                                                                        TreeMap::new,
                                                                                        Collectors.toList()))
                                                         .descendingMap()
                                                         .values()));
    }

    /**
     * Get the longest matching CIDR (i.e, the narrowest match) for given IP
     * @param ip IP to lookup CIDR group
     * @return CIDR group name(s)
     */
    public Set<V> lookupLongestMatchForIP(InetAddress ip)
    {
        // Valid when empty CIDR groups mappings received, i.e, cidr_groups table is empty
        if (tree == null)
            return Collections.emptySet();

        return tree.query(ip);
    }

    /**
     * This class represents a node of an IP interval tree.
     * A node contains a CIDR, value associated with it, left and right children
     */
    static class IPIntervalNode<V>
    {
        private final CIDR cidr;
        private final Set<V> values = new HashSet<>();
        private IPIntervalNode<V>[] left;
        private IPIntervalNode<V>[] right;

        public IPIntervalNode(CIDR cidr, Set<V> values, IPIntervalNode<V>[] children)
        {
            this.cidr = cidr;
            if (values != null)
                this.values.addAll(values);
            updateChildren(children, true, true);
        }

        @VisibleForTesting
        CIDR cidr()
        {
            return cidr;
        }

        @VisibleForTesting
        IPIntervalNode<V>[] left()
        {
            return left;
        }

        @VisibleForTesting
        IPIntervalNode<V>[] right()
        {
            return right;
        }

        private void updateLeft(IPIntervalNode<V>[] newValue, boolean shouldUpdate)
        {
            if (shouldUpdate)
                this.left = newValue;
        }

        private void updateRight(IPIntervalNode<V>[] newValue, boolean shouldUpdate)
        {
            if (shouldUpdate)
                this.right = newValue;
        }

        /**
         * Split the children array according to the IP range of this node, and link the nodes to either
         * the left or right children.
         * @param children array of nodes which are children for this node
         * @param updateLeft true to update left children, false otherwise
         * @param updateRight true to update right children, false otherwise
         */
        private void updateChildren(IPIntervalNode<V>[] children, boolean updateLeft, boolean updateRight)
        {
            // this is leaf node
            if (children == null)
            {
                updateLeft(null, updateLeft);
                updateRight(null, updateRight);
                return;
            }

            // Find the node in the children that is the closest to this node.
            int index = binarySearchNodesIndex(children, this.cidr.getStartIpAddress());
            IPIntervalNode<V> closest = children[index];

            // Scenario - all children nodes are greater than this node
            if (index == 0 && CIDR.compareIPs(this.cidr.getEndIpAddress(), closest.cidr.getStartIpAddress()) < 0)
            {
                updateLeft(null, updateLeft);
                updateRight(children, updateRight);
            }
            // Scenario - all children nodes are lower than this node
            else if (index == children.length - 1 &&
                     CIDR.compareIPs(this.cidr.getStartIpAddress(), closest.cidr.getEndIpAddress()) > 0)
            {
                updateLeft(children, updateLeft);
                updateRight(null, updateRight);
            }
            else // Scenario - part of the children nodes are lower, and the other are greater
            {
                // When this node does not overlap with the closest, split the array and
                // link left and right children correspondingly.
                if (CIDR.compareIPs(this.cidr.getStartIpAddress(), closest.cidr.getEndIpAddress()) > 0)
                {
                    // including the closest (node at index) in left
                    updateLeft(Arrays.copyOfRange(children, 0, index + 1), updateLeft);
                    // put the rest in right
                    updateRight(Arrays.copyOfRange(children, index + 1, children.length), updateRight);
                }
                else // When the node overlaps, include the closest node in both its left and right children nodes.
                {
                    // The parent node overlaps with at most 1 interval in the children, because of nature of the CIDR.
                    // Increasing the bit mask by 1, divides the range into halfs.
                    // Note that the node@index is included in both left and right
                    // it is because the current interval partially overlaps with the closest interval
                    // The overlapping interval should always be searched if we cannot find an exact match with current interval.
                    updateLeft(Arrays.copyOfRange(children, 0, index + 1), updateLeft);
                    updateRight(Arrays.copyOfRange(children, index, children.length), updateRight);
                }
            }
        }

        private void updateLeftIfNull(IPIntervalNode<V>[] children)
        {
            if (left != null)
                return;

            updateChildren(children, true, false);
        }

        private void updateRightIfNull(IPIntervalNode<V>[] children)
        {
            if (right != null)
                return;

            updateChildren(children, false, true);
        }

        /**
         * Binary search given array of nodes and return index of the closest matching node.
         * It looks up for the interval that matches exactly or is left (lower) to the ip.
         * @param nodes array of nodes
         * @param ip    IP address to search
         * @param <V>   data type of the value
         * @return index of the closest node
         */
        static <V> int binarySearchNodesIndex(IPIntervalNode<V>[] nodes, InetAddress ip)
        {
            int start = 0; // inclusive
            int end = nodes.length; // exclusive

            while (true)
            {
                if (start >= end)
                {
                    // return the closest
                    return Math.max((end - 1), 0);
                }

                int mid = start + (end - start) / 2;
                IPIntervalNode<V> midNode = nodes[mid];
                int cmp = CIDR.compareIPs(ip, midNode.cidr.getStartIpAddress());

                if (cmp == 0) // found the node
                {
                    return mid;
                }
                else if (cmp < 0) // Given IP is less than middle node's starting IP, search left side sub array
                {
                    end = mid;
                }
                else  // Given IP is >= middle node's starting IP, so compare ending IP
                {
                    int compEnd = CIDR.compareIPs(ip, midNode.cidr.getEndIpAddress());
                    // Given IP is >= middle node's starting IP and <= than the ending IP, found the match
                    if (compEnd <= 0)
                    {
                        return mid;
                    }
                    else // IP > middle node's end IP >= given IP, search right side sub array
                    {
                        start = mid + 1;
                    }
                }
            }
        }

        /**
         * Binary search given array of nodes to find the closest IP interval to the input IP
         * @param nodes array of nodes
         * @param ip    IP address to search
         * @param <V>   data type of the value
         * @return the closest node to the input IP
         */
        static <V> IPIntervalNode<V> binarySearchNodes(IPIntervalNode<V>[] nodes, InetAddress ip)
        {
            int index = binarySearchNodesIndex(nodes, ip);
            return nodes[index];
        }

        /**
         * Search the tree for a CIDR matching given IP. Uses DFS and stops on first match i.e, finds the closest match
         * to the root, which is the narrowest matching CIDR
         * @param root subtree with this node as root
         * @param ip   IP address to search
         * @param <V>  data type of the value
         * @return value(s) associated with the CIDR matching the given IP, Returns null if no match found
         */
        static <V> Set<V> query(IPIntervalNode<V> root, InetAddress ip)
        {
            IPIntervalNode<V> current = root;
            while (true) // while loop transformed from tail recursion
            {
                boolean largerThanStart = CIDR.compareIPs(ip, current.cidr.getStartIpAddress()) >= 0;
                boolean lessThanEnd = CIDR.compareIPs(ip, current.cidr.getEndIpAddress()) <= 0;
                if (largerThanStart && lessThanEnd)
                {
                    return current.values;
                }
                else
                {
                    IPIntervalNode<V>[] candidates = largerThanStart ? current.right : current.left;
                    // the tree is exhausted, and we are unable to find a match
                    if (candidates == null)
                    {
                        return null;
                    }
                    current = binarySearchNodes(candidates, ip);
                }
            }
        }
    }


    /**
     * This class represents an interval tree for CIDRs
     * @param <V> data type of the value
     */
    static class IPIntervalTree<V>
    {
        // References to first level nodes of the tree
        private final IPIntervalNode<V>[] level0;
        // depth of the tree
        private final int depth;

        private IPIntervalTree(IPIntervalNode<V>[] nodes, int depth)
        {
            this.level0 = nodes;
            this.depth = depth;
        }

        @VisibleForTesting
        int getDepth()
        {
            return depth;
        }

        /**
         * Optimize levels by moving non-overlapping CIDRs from lower level to the upper level. Levels are updated in-place
         * This optimization moves CIDRs closer to the root, hence improves the search to find IP nearer to the root,
         * i.e, avoiding going depth during the search
         * @param upperLevel level for CIDRs with higher netmask value
         * @param lowerLevel level for CIDRs with lower netmask value
         * @param <V>        data type of value
         */
        private static <V> void optimizeLevels(List<Map.Entry<CIDR, V>> upperLevel, List<Map.Entry<CIDR, V>> lowerLevel)
        {
            List<Map.Entry<CIDR, V>> newUpper = new ArrayList<>(upperLevel.size() + lowerLevel.size());
            newUpper.addAll(upperLevel);

            List<Map.Entry<CIDR, V>> newLower = new ArrayList<>(lowerLevel.size());

            for (int i = 0; i < lowerLevel.size(); i++)
            {
                boolean noOverlap = true;
                for (int j = 0; j < upperLevel.size(); j++)
                {
                    if (CIDR.overlaps(lowerLevel.get(i).getKey(), upperLevel.get(j).getKey()))
                    {
                        // overlapping node remains in lower level
                        newLower.add(lowerLevel.get(i));
                        noOverlap = false;
                        break;
                    }
                }

                // the node from lower level does not overlap with any node in upper level, so move it up
                if (noOverlap)
                {
                    newUpper.add(lowerLevel.get(i));
                }
            }

            upperLevel.clear();
            lowerLevel.clear();
            upperLevel.addAll(newUpper);
            lowerLevel.addAll(newLower);
        }

        /**
         * Optimize by lifting lower level non-overlapping CIDRs to the upper levels.
         * The method modifies cidrsGroupedByNetMasks in-place.
         * @param cidrsGroupedByNetMasks CIDRs grouped by netmask, levels in the order of higher netmask to lower netmask value
         * @param <V>                    data type of the value
         */
        private static <V> void optimizeAllLevels(List<List<Map.Entry<CIDR, Set<V>>>> cidrsGroupedByNetMasks)
        {
            for (int i = 0; i < cidrsGroupedByNetMasks.size(); i++)
            {
                List<Map.Entry<CIDR, Set<V>>> current = cidrsGroupedByNetMasks.get(0);
                for (int j = i + 1; j < cidrsGroupedByNetMasks.size(); j++)
                {
                    List<Map.Entry<CIDR, Set<V>>> lower = cidrsGroupedByNetMasks.get(j);
                    optimizeLevels(current, lower);
                }
            }
        }

        /**
         * Link the nodes between levels. If a node in the lower level has no left or right children,
         * the nodes from higher levels are lifted and linked as the left or right children.
         * @param cidrMappings list of CIDR to value mappings
         * @param result       2 dimension array that stores all (linked) nodes in the tree. result[0] is the first level in the tree.
         * @param startIndex   starting level in the result array, to link it to its children nodes
         * @param <V>          data type of the value
         */
        @SuppressWarnings("unchecked")
        private static <V> void linkNodes(List<List<Map.Entry<CIDR, Set<V>>>> cidrMappings,
                                          IPIntervalNode<V>[][] result,
                                          int startIndex)
        {
            List<Map.Entry<CIDR, Set<V>>> cidrsAtLevel = cidrMappings.get(startIndex);
            int next = startIndex + 1;
            IPIntervalNode<V>[] lowerLevel = next == result.length ? null : result[next];
            result[startIndex] = cidrsAtLevel
            .stream()
            .map(pair -> {
                CIDR cidr = pair.getKey();
                Set<V> value = pair.getValue();
                IPIntervalNode<V> node = new IPIntervalNode<>(cidr, value, lowerLevel);
                // Cannot link its left or right children to the next level nodes.
                // Try the nodes in the even lower levels.
                if (next + 1 < result.length && (node.left == null || node.right == null))
                {
                    for (int i = next + 1; i < result.length; i++)
                    {
                        node.updateLeftIfNull(result[i]);
                        node.updateRightIfNull(result[i]);

                        // the node has both left and right children, it is good and no further lifting is needed.
                        if (node.left != null && node.right != null)
                        {
                            break;
                        }
                    }
                }
                return node;
            })
            // sort by ipStart. Remember the interval do not overlap
            .sorted(Comparator.comparing(n -> n.cidr.getStartIpAddress(), CIDR::compareIPs))
            .toArray(IPIntervalNode[]::new);
        }

        /**
         * Build an interval tree for given CIDRs
         * @param cidrsGroupedByNetMasks CIDRs grouped by netmask, levels in the order of higher netmask to lower netmask value
         * @param <V>                    data type of the value
         * @return returns reference to the interval tree, returns null if input is empty
         */
        @SuppressWarnings("unchecked")
        public static <V> IPIntervalTree<V> build(List<List<Map.Entry<CIDR, Set<V>>>> cidrsGroupedByNetMasks)
        {
            if (cidrsGroupedByNetMasks.isEmpty())
                return null;

            optimizeAllLevels(cidrsGroupedByNetMasks);

            // After optimization, trim empty levels if exists
            cidrsGroupedByNetMasks.removeIf(List::isEmpty);

            // Create an array for each level
            IPIntervalNode<V>[][] result = new IPIntervalNode[cidrsGroupedByNetMasks.size()][];
            // build the tree bottom up, i.e, add CIDRs in the order of smaller netmask value to higher netmask value
            for (int i = cidrsGroupedByNetMasks.size() - 1; i >= 0; i--)
            {
                linkNodes(cidrsGroupedByNetMasks, result, i);
            }
            return new IPIntervalTree<>(result[0], cidrsGroupedByNetMasks.size());
        }

        /**
         * Search interval tree for the longest matching CIDR for the given IP
         * @param ip IP address to search
         * @return Value(s) associated with matching CIDR
         */
        public Set<V> query(InetAddress ip)
        {
            IPIntervalNode<V> closest = IPIntervalNode.binarySearchNodes(level0, ip);
            return IPIntervalNode.query(closest, ip);
        }
    }
}
