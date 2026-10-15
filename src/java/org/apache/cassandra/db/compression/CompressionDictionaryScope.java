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

package org.apache.cassandra.db.compression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.LocalizeString;

/**
 * The nodes which compress with a compression dictionary. A scope is either the whole cluster or a union of places,
 * stored in the {@code scope frozen<set<frozen<map<text, text>>>>} column of
 * {@code system_distributed.compression_dictionaries}:
 *
 * <ul>
 *     <li>{@code null}: the whole cluster, every node</li>
 *     <li>{@code {{'node_id': '<id>'}, ...}}: the nodes of the places, a place being one node by its id in cluster
 *     metadata</li>
 * </ul>
 *
 * A node compresses with the newest dictionary of a table whose scope contains it. Other kinds of places, e.g. a rack
 * or a datacenter, can be added later as maps with other keys. A place this version does not understand, e.g. one
 * written by a later version, contains no node, so it is never misread as one which contains this node, and it is not
 * considered orphaned either. An empty set is never written.
 * <p>
 * A dictionary is promoted by widening its scope, see {@link #checkCanBeWidenedTo(CompressionDictionaryScope)}: the
 * wider scope contains every place of the narrower one, a scope never shrinks.
 */
public final class CompressionDictionaryScope
{
    public static final String NODE_ID_KEY = "node_id";

    /**
     * Type of the scope column.
     */
    public static final SetType<Map<String, String>> TYPE = SetType.getInstance(MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false), false);

    /**
     * The whole cluster, stored as null.
     */
    public static final CompressionDictionaryScope CLUSTER = new CompressionDictionaryScope(null);

    /**
     * The kinds of scope an operator can train or import a dictionary with.
     */
    public enum Kind
    {
        NODE,
        CLUSTER;

        /**
         * @param node id of the node which trains or imports the dictionary
         * @return the scope of this kind which contains the given node
         * @throws IllegalStateException if the scope is a node and the node id is not known
         */
        public CompressionDictionaryScope of(@Nullable NodeId node)
        {
            switch (this)
            {
                case NODE:
                    if (node == null)
                        throw new IllegalStateException("The id of this node is not known, a dictionary can not be scoped to it");
                    return CompressionDictionaryScope.node(node);
                case CLUSTER:
                    return CompressionDictionaryScope.CLUSTER;
                default:
                    throw new AssertionError("Unknown kind of scope " + this);
            }
        }

        public String displayName()
        {
            return LocalizeString.toLowerCaseLocalized(name());
        }

        /**
         * @param value name of a kind, case-insensitive
         * @return the kind of the given name
         * @throws IllegalArgumentException if there is no such kind
         */
        public static Kind parse(String value)
        {
            if (value != null)
            {
                for (Kind kind : values())
                {
                    if (kind.displayName().equals(LocalizeString.toLowerCaseLocalized(value.trim())))
                        return kind;
                }
            }

            throw new IllegalArgumentException(String.format("Invalid scope '%s', valid scopes are %s",
                                                             value, Arrays.toString(Arrays.stream(values()).map(Kind::displayName).toArray())));
        }
    }

    /**
     * One place of a scope, an element of the scope column.
     */
    public static abstract class Place implements Comparable<Place>
    {
        /**
         * @return the element of the scope column standing for this place
         */
        public abstract Map<String, String> toMap();

        /**
         * @return true when this version knows what nodes the place contains
         */
        public abstract boolean isKnown();

        /**
         * @param node id of a node
         * @return true when the node is one of this place
         */
        abstract boolean contains(NodeId node);

        /**
         * @param other a place
         * @return true when every node of the other place is one of this place, regardless of the topology of the
         *         cluster; a place covers itself
         */
        boolean covers(Place other)
        {
            return equals(other);
        }

        /**
         * @return endpoints of the nodes of this place in the cluster
         */
        abstract Collection<InetAddressAndPort> memberEndpoints(Directory directory);

        /**
         * @return true unless no node of this place is in the cluster anymore
         */
        abstract boolean hasMembers(Directory directory);

        // the order places are listed in: nodes by id, then the places this version does not know
        abstract int rank();

        @Override
        public int compareTo(Place other)
        {
            int byRank = Integer.compare(rank(), other.rank());
            return byRank != 0 ? byRank : compareWithinRank(other);
        }

        abstract int compareWithinRank(Place other);

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return toMap().equals(((Place) o).toMap());
        }

        @Override
        public int hashCode()
        {
            return toMap().hashCode();
        }

        /**
         * @param map an element of the scope column
         * @return the place the element stands for, a place this version does not know when it does not understand it
         */
        public static Place fromMap(Map<String, String> map)
        {
            if (map.size() == 1 && map.containsKey(NODE_ID_KEY))
            {
                try
                {
                    return new NodePlace(new NodeId(Integer.parseInt(map.get(NODE_ID_KEY))));
                }
                catch (NumberFormatException e)
                {
                    // not a node id this version knows
                }
            }
            return new UnknownPlace(map);
        }
    }

    /**
     * A single node, by its id in cluster metadata.
     */
    public static final class NodePlace extends Place
    {
        public final NodeId node;

        public NodePlace(NodeId node)
        {
            this.node = Objects.requireNonNull(node, "node id");
        }

        @Override
        public Map<String, String> toMap()
        {
            return Map.of(NODE_ID_KEY, Integer.toString(node.id()));
        }

        @Override
        public boolean isKnown()
        {
            return true;
        }

        @Override
        boolean contains(NodeId node)
        {
            return this.node.equals(node);
        }

        @Override
        Collection<InetAddressAndPort> memberEndpoints(Directory directory)
        {
            if (!isInCluster(directory, node))
                return List.of();

            InetAddressAndPort endpoint = directory.endpoint(node);
            return endpoint == null ? List.of() : List.of(endpoint);
        }

        @Override
        boolean hasMembers(Directory directory)
        {
            return isInCluster(directory, node);
        }

        @Override
        int rank()
        {
            return 0;
        }

        @Override
        int compareWithinRank(Place other)
        {
            return Integer.compare(node.id(), ((NodePlace) other).node.id());
        }

        @Override
        public String toString()
        {
            return Kind.NODE.displayName() + ' ' + node.id();
        }
    }

    /**
     * A place this version does not understand, e.g. one written by a later version. It contains no node and it is not
     * considered orphaned.
     */
    public static final class UnknownPlace extends Place
    {
        private final Map<String, String> map;

        UnknownPlace(Map<String, String> map)
        {
            this.map = Collections.unmodifiableMap(new TreeMap<>(map));
        }

        @Override
        public Map<String, String> toMap()
        {
            return map;
        }

        @Override
        public boolean isKnown()
        {
            return false;
        }

        @Override
        boolean contains(NodeId node)
        {
            return false;
        }

        @Override
        Collection<InetAddressAndPort> memberEndpoints(Directory directory)
        {
            return List.of();
        }

        @Override
        boolean hasMembers(Directory directory)
        {
            return true;
        }

        @Override
        int rank()
        {
            return 1;
        }

        @Override
        int compareWithinRank(Place other)
        {
            return map.toString().compareTo(((UnknownPlace) other).map.toString());
        }

        @Override
        public String toString()
        {
            return map.toString();
        }
    }

    // null for the whole cluster, otherwise unmodifiable and sorted, so the string form is stable
    @Nullable
    private final Set<Place> places;

    private CompressionDictionaryScope(@Nullable Collection<Place> places)
    {
        this.places = places == null ? null : Collections.unmodifiableSet(new LinkedHashSet<>(new TreeSet<>(places)));
    }

    /**
     * @return the scope of a single node
     */
    public static CompressionDictionaryScope node(NodeId node)
    {
        return of(List.of(new NodePlace(node)));
    }

    /**
     * @param places places of the scope
     * @return the union of the places
     */
    public static CompressionDictionaryScope of(Collection<? extends Place> places)
    {
        return new CompressionDictionaryScope(new ArrayList<>(places));
    }

    /**
     * @param value value of the scope column, null for the whole cluster
     * @return the scope the given value stands for
     */
    public static CompressionDictionaryScope fromValue(@Nullable Collection<Map<String, String>> value)
    {
        if (value == null)
            return CLUSTER;

        return new CompressionDictionaryScope(value.stream().map(Place::fromMap).collect(Collectors.toList()));
    }

    /**
     * @return value of the scope column, null for the whole cluster
     */
    @Nullable
    public Set<Map<String, String>> toValue()
    {
        if (places == null)
            return null;

        Set<Map<String, String>> value = new LinkedHashSet<>();
        for (Place place : places)
            value.add(place.toMap());
        return value;
    }

    /**
     * @return the scope stored in the scope column of the row, the whole cluster when it is null
     */
    public static CompressionDictionaryScope fromRow(UntypedResultSet.Row row)
    {
        return row.has("scope") ? fromValue(row.getFrozenSet("scope", TYPE.getElementsType())) : CLUSTER;
    }

    /**
     * @param flattened per place, its keys and values one after another; null for the whole cluster
     * @return the scope the given places stand for
     * @throws IllegalArgumentException if a place has a key without a value
     */
    public static CompressionDictionaryScope fromFlattened(@Nullable String[][] flattened)
    {
        if (flattened == null)
            return CLUSTER;

        List<Map<String, String>> value = new ArrayList<>();
        for (String[] place : flattened)
        {
            if (place.length % 2 != 0)
                throw new IllegalArgumentException("Place of a scope has a key without a value: " + Arrays.toString(place));

            Map<String, String> map = new TreeMap<>();
            for (int i = 0; i < place.length; i += 2)
                map.put(place[i], place[i + 1]);
            value.add(map);
        }
        return fromValue(value);
    }

    /**
     * Per place, its keys and values one after another, a form JMX open types can carry.
     *
     * @return the places in their order, keys of a place ordered, null for the whole cluster
     */
    @Nullable
    public String[][] toFlattened()
    {
        if (places == null)
            return null;

        String[][] flattened = new String[places.size()][];
        int p = 0;
        for (Place place : places)
        {
            String[] entries = new String[place.toMap().size() * 2];
            int i = 0;
            for (Map.Entry<String, String> entry : new TreeMap<>(place.toMap()).entrySet())
            {
                entries[i++] = entry.getKey();
                entries[i++] = entry.getValue();
            }
            flattened[p++] = entries;
        }
        return flattened;
    }

    public boolean isCluster()
    {
        return places == null;
    }

    /**
     * @return the places of the scope, empty for the whole cluster
     */
    public Set<Place> places()
    {
        return places == null ? Set.of() : places;
    }

    /**
     * @return true when this version knows what nodes the scope contains, i.e. it is the whole cluster or it has
     *         places, all of which this version knows
     */
    public boolean isKnown()
    {
        return places == null || (!places.isEmpty() && places.stream().allMatch(Place::isKnown));
    }

    /**
     * @param node id of a node, null when it is not known
     * @return true when the given node compresses with a dictionary of this scope, i.e. the scope is the whole
     *         cluster or one of its places contains the node
     */
    public boolean contains(@Nullable NodeId node)
    {
        if (places == null)
            return true;

        return node != null && places.stream().anyMatch(place -> place.contains(node));
    }

    /**
     * @param other a scope
     * @return true when every node of the other scope is one of this scope regardless of the topology of the cluster,
     *         i.e. this is the whole cluster, or each place of the other scope is covered by a place of this scope
     */
    public boolean covers(CompressionDictionaryScope other)
    {
        if (places == null)
            return true;
        if (other.places == null)
            return false;

        return other.places.stream().allMatch(place -> places.stream().anyMatch(own -> own.covers(place)));
    }

    /**
     * Checks that a dictionary of this scope can be promoted to the given one, i.e. the given scope is wider: it
     * covers this scope, it is not the same, and this version knows all its places. A scope can not shrink, and it
     * can not be replaced with a disjoint or partially overlapping one either.
     *
     * @param wider the scope to promote to
     * @throws IllegalArgumentException if a dictionary of this scope can not be promoted to the given one
     */
    public void checkCanBeWidenedTo(CompressionDictionaryScope wider)
    {
        if (!wider.isKnown())
            throw new IllegalArgumentException(String.format("Scope %s contains places this version does not know, a dictionary can not be promoted to it", wider));

        if (wider.equals(this))
            throw new IllegalArgumentException(String.format("The dictionary is already scoped to %s", this));

        if (!wider.covers(this))
            throw new IllegalArgumentException(String.format("Scope %s does not contain all of scope %s, a dictionary can only be promoted to a wider scope",
                                                             wider, this));
    }

    /**
     * @param directory directory of the cluster
     * @return endpoints of the nodes of this scope which can receive a message about a dictionary of it
     */
    public Collection<InetAddressAndPort> memberEndpoints(Directory directory)
    {
        if (places == null)
            return directory.allJoinedEndpoints();

        Set<InetAddressAndPort> endpoints = new LinkedHashSet<>();
        for (Place place : places)
            endpoints.addAll(place.memberEndpoints(directory));
        return endpoints;
    }

    /**
     * A dictionary whose scope has no member is orphaned, nothing compresses with it anymore.
     *
     * @param directory directory of the cluster
     * @return false when no place of this scope has a node in the cluster anymore, e.g. the node of a single node
     *         scope left; true for the whole cluster, and for a scope with places this version does not understand
     */
    public boolean hasMembers(Directory directory)
    {
        if (places == null || places.isEmpty())
            return true;

        return places.stream().anyMatch(place -> place.hasMembers(directory));
    }

    private static boolean isInCluster(Directory directory, NodeId node)
    {
        NodeState state = directory.peerState(node);
        return state != null && state != NodeState.LEFT;
    }

    /**
     * @return "cluster", or the places, e.g. "node 1, node 2"
     */
    @Override
    public String toString()
    {
        if (places == null)
            return Kind.CLUSTER.displayName();

        return places.stream().map(Place::toString).collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(places, ((CompressionDictionaryScope) o).places);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(places);
    }
}
