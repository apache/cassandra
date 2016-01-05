package org.apache.cassandra.locator;

import com.google.common.collect.Iterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;

public class PendingRangeMaps implements Iterable<Map.Entry<Range<Token>, List<InetAddress>>>
{
    private static final Logger logger = LoggerFactory.getLogger(PendingRangeMaps.class);

    /**
     * We have for NavigableMap to be able to search for ranges containing a token efficiently.
     *
     * First two are for non-wrap-around ranges, and the last two are for wrap-around ranges.
     */
    // ascendingMap will sort the ranges by the ascending order of right token
    final NavigableMap<Range<Token>, List<InetAddress>> ascendingMap;
    /**
     * sorting end ascending, if ends are same, sorting begin descending, so that token (end, end) will
     * come before (begin, end] with the same end, and (begin, end) will be selected in the tailMap.
     */
    static final Comparator<Range<Token>> ascendingComparator = new Comparator<Range<Token>>()
        {
            @Override
            public int compare(Range<Token> o1, Range<Token> o2)
            {
                int res = o1.right.compareTo(o2.right);
                if (res != 0)
                    return res;

                return o2.left.compareTo(o1.left);
            }
        };

    // ascendingMap will sort the ranges by the descending order of left token
    final NavigableMap<Range<Token>, List<InetAddress>> descendingMap;
    /**
     * sorting begin descending, if begins are same, sorting end descending, so that token (begin, begin) will
     * come after (begin, end] with the same begin, and (begin, end) won't be selected in the tailMap.
     */
    static final Comparator<Range<Token>> descendingComparator = new Comparator<Range<Token>>()
        {
            @Override
            public int compare(Range<Token> o1, Range<Token> o2)
            {
                int res = o2.left.compareTo(o1.left);
                if (res != 0)
                    return res;

                // if left tokens are same, sort by the descending of the right tokens.
                return o2.right.compareTo(o1.right);
            }
        };

    // these two maps are for warp around ranges.
    final NavigableMap<Range<Token>, List<InetAddress>> ascendingMapForWrapAround;
    /**
     * for wrap around range (begin, end], which begin > end.
     * Sorting end ascending, if ends are same, sorting begin ascending,
     * so that token (end, end) will come before (begin, end] with the same end, and (begin, end] will be selected in
     * the tailMap.
     */
    static final Comparator<Range<Token>> ascendingComparatorForWrapAround = new Comparator<Range<Token>>()
    {
        @Override
        public int compare(Range<Token> o1, Range<Token> o2)
        {
            int res = o1.right.compareTo(o2.right);
            if (res != 0)
                return res;

            return o1.left.compareTo(o2.left);
        }
    };

    final NavigableMap<Range<Token>, List<InetAddress>> descendingMapForWrapAround;
    /**
     * for wrap around ranges, which begin > end.
     * Sorting end ascending, so that token (begin, begin) will come after (begin, end] with the same begin,
     * and (begin, end) won't be selected in the tailMap.
     */
    static final Comparator<Range<Token>> descendingComparatorForWrapAround = new Comparator<Range<Token>>()
    {
        @Override
        public int compare(Range<Token> o1, Range<Token> o2)
        {
            int res = o2.left.compareTo(o1.left);
            if (res != 0)
                return res;
            return o1.right.compareTo(o2.right);
        }
    };

    public PendingRangeMaps()
    {
        this.ascendingMap = new TreeMap<Range<Token>, List<InetAddress>>(ascendingComparator);
        this.descendingMap = new TreeMap<Range<Token>, List<InetAddress>>(descendingComparator);
        this.ascendingMapForWrapAround = new TreeMap<Range<Token>, List<InetAddress>>(ascendingComparatorForWrapAround);
        this.descendingMapForWrapAround = new TreeMap<Range<Token>, List<InetAddress>>(descendingComparatorForWrapAround);
    }

    static final void addToMap(Range<Token> range,
                               InetAddress address,
                               NavigableMap<Range<Token>, List<InetAddress>> ascendingMap,
                               NavigableMap<Range<Token>, List<InetAddress>> descendingMap)
    {
        List<InetAddress> addresses = ascendingMap.get(range);
        if (addresses == null)
        {
            addresses = new ArrayList<InetAddress>(1);
            ascendingMap.put(range, addresses);
            descendingMap.put(range, addresses);
        }
        addresses.add(address);
    }

    public void addPendingRange(Range<Token> range, InetAddress address)
    {
        if (Range.isWrapAround(range.left, range.right))
        {
            addToMap(range, address, ascendingMapForWrapAround, descendingMapForWrapAround);
        }
        else
        {
            addToMap(range, address, ascendingMap, descendingMap);
        }
    }

    static final void addIntersections(Set<InetAddress> endpointsToAdd,
                                       NavigableMap<Range<Token>, List<InetAddress>> smallerMap,
                                       NavigableMap<Range<Token>, List<InetAddress>> biggerMap)
    {
        // find the intersection of two sets
        for (Range<Token> range : smallerMap.keySet())
        {
            List<InetAddress> addresses = biggerMap.get(range);
            if (addresses != null)
            {
                endpointsToAdd.addAll(addresses);
            }
        }
    }

    public Collection<InetAddress> pendingEndpointsFor(Token token)
    {
        Set<InetAddress> endpoints = new HashSet<>();

        Range searchRange = new Range(token, token);

        // search for non-wrap-around maps
        NavigableMap<Range<Token>, List<InetAddress>> ascendingTailMap = ascendingMap.tailMap(searchRange, true);
        NavigableMap<Range<Token>, List<InetAddress>> descendingTailMap = descendingMap.tailMap(searchRange, false);

        // add intersections of two maps
        if (ascendingTailMap.size() < descendingTailMap.size())
        {
            addIntersections(endpoints, ascendingTailMap, descendingTailMap);
        }
        else
        {
            addIntersections(endpoints, descendingTailMap, ascendingTailMap);
        }

        // search for wrap-around sets
        ascendingTailMap = ascendingMapForWrapAround.tailMap(searchRange, true);
        descendingTailMap = descendingMapForWrapAround.tailMap(searchRange, false);

        // add them since they are all necessary.
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : ascendingTailMap.entrySet())
        {
            endpoints.addAll(entry.getValue());
        }
        for (Map.Entry<Range<Token>, List<InetAddress>> entry : descendingTailMap.entrySet())
        {
            endpoints.addAll(entry.getValue());
        }

        return endpoints;
    }

    public String printPendingRanges()
    {
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Range<Token>, List<InetAddress>> entry : this)
        {
            Range<Token> range = entry.getKey();

            for (InetAddress address : entry.getValue())
            {
                sb.append(address).append(':').append(range);
                sb.append(System.getProperty("line.separator"));
            }
        }

        return sb.toString();
    }

    @Override
    public Iterator<Map.Entry<Range<Token>, List<InetAddress>>> iterator()
    {
        return Iterators.concat(ascendingMap.entrySet().iterator(), ascendingMapForWrapAround.entrySet().iterator());
    }
}
