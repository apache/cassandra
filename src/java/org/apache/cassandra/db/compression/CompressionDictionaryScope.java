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

import com.google.common.collect.Multimap;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.Location;
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
 *     <li>a set of places, the nodes of all of them:
 *     <ul>
 *         <li>{@code {'node_id': '<id>'}}: one node, by its id in cluster metadata</li>
 *         <li>{@code {'dc': '<dc>', 'rack': '<rack>'}}: the nodes of a rack of a datacenter</li>
 *         <li>{@code {'dc': '<dc>'}}: the nodes of a datacenter</li>
 *     </ul>
 *     </li>
 * </ul>
 *
 * A node compresses with the newest dictionary of a table whose scope contains it. Whether a rack or a datacenter
 * contains a node is decided by the location of the node in cluster metadata at the time it is asked, so a node moved
 * to another rack follows the scope of its new rack. A place this version does not understand, e.g. one written by a
 * later version, contains no node, so it is never misread as one which contains this node, and it is not considered
 * orphaned either. An empty set is never written.
 * <p>
 * A dictionary is promoted by widening its scope, see
 * {@link #checkCanBeWidenedTo(Directory, CompressionDictionaryScope)}: the wider scope contains every place of the
 * narrower one, a scope never shrinks.
 */
public final class CompressionDictionaryScope
{
    public static final String NODE_ID_KEY = "node_id";
    public static final String DC_KEY = "dc";
    public static final String RACK_KEY = "rack";

    /**
     * Type of the scope column.
     */
    public static final SetType<Map<String, String>> TYPE = SetType.getInstance(MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false), false);

    /**
     * The whole cluster, stored as null.
     */
    public static final CompressionDictionaryScope CLUSTER = new CompressionDictionaryScope(null);

    /**
     * The kinds of scope an operator can train or import a dictionary with, relative to the node which trains or
     * imports it.
     */
    public enum Kind
    {
        NODE,
        RACK,
        DC,
        CLUSTER;

        /**
         * @param directory directory of the cluster, null when there is no cluster metadata
         * @param node      id of the node which trains or imports the dictionary, null when it is not known
         * @return the scope of this kind which contains the given node: the node, its rack, its datacenter (dc) or
         *         the whole cluster
         * @throws IllegalStateException if the scope is not the whole cluster and the node or its location is not known
         */
        public CompressionDictionaryScope of(@Nullable Directory directory, @Nullable NodeId node)
        {
            if (this == CLUSTER)
                return CompressionDictionaryScope.CLUSTER;

            if (node == null)
                throw new IllegalStateException(String.format("The id of this node is not known, a dictionary can not be scoped to its %s", displayName()));

            if (this == NODE)
                return CompressionDictionaryScope.of(List.of(new NodePlace(node)));

            Location location = directory == null ? null : directory.location(node);
            if (location == null)
                throw new IllegalStateException(String.format("The location of this node is not known, a dictionary can not be scoped to its %s", displayName()));

            return CompressionDictionaryScope.of(List.of(this == RACK ? new RackPlace(location.datacenter, location.rack)
                                                                      : new DatacenterPlace(location.datacenter)));
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
        public boolean isKnown()
        {
            return true;
        }

        /**
         * @param directory directory of the cluster, null when there is no cluster metadata
         * @param node      id of a node
         * @return true when the node is one of this place
         */
        abstract boolean contains(@Nullable Directory directory, NodeId node);

        /**
         * @param directory directory of the cluster, which tells the location of a node
         * @param other     a place
         * @return true when every node of the other place is one of this place: a place covers itself, a datacenter
         *         covers its racks, and a rack or a datacenter covers a node located in it
         */
        boolean covers(Directory directory, Place other)
        {
            return equals(other);
        }

        /**
         * @return endpoints of the joined nodes of this place
         */
        abstract Collection<InetAddressAndPort> memberEndpoints(Directory directory);

        /**
         * @return true unless no node of this place is in the cluster anymore
         */
        abstract boolean hasMembers(Directory directory);

        // the order places are listed in: nodes by id, racks, datacenters, then the places this version does not know
        abstract int rank();

        abstract int compareWithinRank(Place other);

        @Override
        public int compareTo(Place other)
        {
            int byRank = Integer.compare(rank(), other.rank());
            return byRank != 0 ? byRank : compareWithinRank(other);
        }

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
            String datacenter = map.get(DC_KEY);
            if (map.size() == 1 && map.containsKey(NODE_ID_KEY))
            {
                try
                {
                    return new NodePlace(new NodeId(Integer.parseInt(map.get(NODE_ID_KEY))));
                }
                catch (NumberFormatException e)
                {
                    return new UnknownPlace(map);
                }
            }
            if (map.size() == 2 && datacenter != null && map.get(RACK_KEY) != null)
                return new RackPlace(datacenter, map.get(RACK_KEY));
            if (map.size() == 1 && datacenter != null)
                return new DatacenterPlace(datacenter);
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
        boolean contains(@Nullable Directory directory, NodeId node)
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
     * The nodes of a rack of a datacenter.
     */
    public static final class RackPlace extends Place
    {
        public final String datacenter;
        public final String rack;

        public RackPlace(String datacenter, String rack)
        {
            this.datacenter = Objects.requireNonNull(datacenter, "datacenter");
            this.rack = Objects.requireNonNull(rack, "rack");
        }

        @Override
        public Map<String, String> toMap()
        {
            return Map.of(DC_KEY, datacenter, RACK_KEY, rack);
        }

        @Override
        boolean contains(@Nullable Directory directory, NodeId node)
        {
            return directory != null && isIn(directory.location(node));
        }

        @Override
        boolean covers(Directory directory, Place other)
        {
            if (other instanceof NodePlace)
                return isIn(directory.location(((NodePlace) other).node));

            return equals(other);
        }

        private boolean isIn(@Nullable Location location)
        {
            return location != null && datacenter.equals(location.datacenter) && rack.equals(location.rack);
        }

        @Override
        Collection<InetAddressAndPort> memberEndpoints(Directory directory)
        {
            Multimap<String, InetAddressAndPort> racks = directory.datacenterRacks(datacenter);
            return racks == null ? List.of() : racks.get(rack);
        }

        @Override
        boolean hasMembers(Directory directory)
        {
            return directory.peerIds().stream().anyMatch(node -> isInCluster(directory, node) && isIn(directory.location(node)));
        }

        @Override
        int rank()
        {
            return 1;
        }

        @Override
        int compareWithinRank(Place other)
        {
            RackPlace place = (RackPlace) other;
            int byDatacenter = datacenter.compareTo(place.datacenter);
            return byDatacenter != 0 ? byDatacenter : rack.compareTo(place.rack);
        }

        @Override
        public String toString()
        {
            return Kind.RACK.displayName() + ' ' + datacenter + '/' + rack;
        }
    }

    /**
     * The nodes of a datacenter.
     */
    public static final class DatacenterPlace extends Place
    {
        public final String datacenter;

        public DatacenterPlace(String datacenter)
        {
            this.datacenter = Objects.requireNonNull(datacenter, "datacenter");
        }

        @Override
        public Map<String, String> toMap()
        {
            return Map.of(DC_KEY, datacenter);
        }

        @Override
        boolean contains(@Nullable Directory directory, NodeId node)
        {
            return directory != null && isIn(directory.location(node));
        }

        @Override
        boolean covers(Directory directory, Place other)
        {
            if (other instanceof NodePlace)
                return isIn(directory.location(((NodePlace) other).node));
            if (other instanceof RackPlace)
                return datacenter.equals(((RackPlace) other).datacenter);

            return equals(other);
        }

        private boolean isIn(@Nullable Location location)
        {
            return location != null && datacenter.equals(location.datacenter);
        }

        @Override
        Collection<InetAddressAndPort> memberEndpoints(Directory directory)
        {
            Collection<InetAddressAndPort> endpoints = directory.datacenterEndpoints(datacenter);
            return endpoints == null ? List.of() : endpoints;
        }

        @Override
        boolean hasMembers(Directory directory)
        {
            return directory.peerIds().stream().anyMatch(node -> isInCluster(directory, node) && isIn(directory.location(node)));
        }

        @Override
        int rank()
        {
            return 2;
        }

        @Override
        int compareWithinRank(Place other)
        {
            return datacenter.compareTo(((DatacenterPlace) other).datacenter);
        }

        @Override
        public String toString()
        {
            return Kind.DC.displayName() + ' ' + datacenter;
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
        boolean contains(@Nullable Directory directory, NodeId node)
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
            return 3;
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
     * @return the union of the places, as given
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
     * @param directory directory of the cluster, which tells the location of the node; null when there is no cluster
     *                  metadata, then only the whole cluster and a node place can contain the node
     * @param node      id of a node, null when it is not known
     * @return true when the given node compresses with a dictionary of this scope, i.e. the scope is the whole
     *         cluster or one of its places contains the node
     */
    public boolean contains(@Nullable Directory directory, @Nullable NodeId node)
    {
        if (places == null)
            return true;

        return node != null && places.stream().anyMatch(place -> place.contains(directory, node));
    }

    /**
     * @param directory directory of the cluster, which tells the location of a node
     * @param other     a scope
     * @return true when every node of the other scope is one of this scope, i.e. this is the whole cluster, or each
     *         place of the other scope is covered by a place of this scope
     */
    public boolean covers(Directory directory, CompressionDictionaryScope other)
    {
        if (places == null)
            return true;
        if (other.places == null)
            return false;

        return other.places.stream().allMatch(place -> covered(directory, places, place));
    }

    private static boolean covered(Directory directory, Collection<Place> places, Place place)
    {
        return places.stream().anyMatch(own -> own.covers(directory, place));
    }

    /**
     * Widens this scope by the given places: the union of the places of this scope and the given ones, without the
     * places which another of them covers, e.g. a rack of a datacenter which is in the scope too, or a node located in
     * a rack or a datacenter which is in the scope too.
     *
     * @param directory directory of the cluster, which tells the location of a node
     * @param add       places to add
     * @return the widened scope, the whole cluster when this is the whole cluster
     */
    public CompressionDictionaryScope plus(Directory directory, Collection<? extends Place> add)
    {
        if (places == null)
            return CLUSTER;

        Set<Place> union = new LinkedHashSet<>(places);
        union.addAll(add);
        List<Place> normalized = new ArrayList<>();
        for (Place place : union)
        {
            boolean coveredByAnother = union.stream().anyMatch(other -> !other.equals(place) && other.covers(directory, place));
            if (!coveredByAnother)
                normalized.add(place);
        }
        return of(normalized);
    }

    /**
     * Checks that a dictionary of this scope can be promoted to the given one, i.e. the given scope is wider: it
     * covers this scope, it is not the same, and this version knows all its places. A scope can not shrink, and it
     * can not be replaced with a disjoint or partially overlapping one either.
     *
     * @param directory directory of the cluster, which tells the location of a node
     * @param wider     the scope to promote to
     * @throws IllegalArgumentException if a dictionary of this scope can not be promoted to the given one
     */
    public void checkCanBeWidenedTo(Directory directory, CompressionDictionaryScope wider)
    {
        if (!wider.isCluster() && wider.places.isEmpty())
            throw new IllegalArgumentException("a scope without places contains no node, a dictionary can not be promoted to it");

        if (!wider.isKnown())
        {
            List<Place> unknown = wider.places.stream().filter(place -> !place.isKnown()).collect(Collectors.toList());
            throw new IllegalArgumentException(String.format("scope %s contains places this version does not know: %s",
                                                             wider, describe(unknown)));
        }

        if (wider.equals(this))
            throw new IllegalArgumentException(String.format("the scope would stay %s, nothing to promote", this));

        if (isCluster())
            throw new IllegalArgumentException(String.format("scope %s would no longer contain the whole cluster, a scope can only be widened", wider));

        List<Place> lost = places.stream().filter(place -> !covered(directory, wider.places(), place)).collect(Collectors.toList());
        if (!wider.isCluster() && !lost.isEmpty())
            throw new IllegalArgumentException(String.format("scope %s would no longer contain %s, a scope can only be widened",
                                                             wider, describe(lost)));
    }

    /**
     * @param directory directory of the cluster, which tells the location of a node
     * @param places    places to check
     * @return the given places which this scope covers already, all of them for the whole cluster
     */
    public List<Place> coveredOf(Directory directory, Collection<? extends Place> places)
    {
        if (isCluster())
            return new ArrayList<>(places);

        List<Place> covered = new ArrayList<>();
        for (Place place : places)
        {
            if (covered(directory, this.places, place))
                covered.add(place);
        }
        return covered;
    }

    /**
     * @return the places as listed in messages, e.g. "rack dc1/r1, dc dc2"
     */
    public static String describe(Collection<? extends Place> places)
    {
        return places.stream().sorted().map(Place::toString).collect(Collectors.joining(", "));
    }

    /**
     * @param directory directory of the cluster
     * @return endpoints of the joined nodes of this scope, which can receive a message about a dictionary of it
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
     *         scope left, or the rack of a rack scope was decommissioned; true for the whole cluster, and for a scope
     *         with places this version does not understand
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
     * @return "cluster", or the places, e.g. "node 1, rack dc1/r1, datacenter dc2"
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
