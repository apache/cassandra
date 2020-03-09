package org.apache.cassandra.utils;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class IntervalList<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static final IntervalList EMPTY_TREE = new IntervalList(Collections.emptyList());

	private final List<Pair<C>> minOrderedIntervals = new ArrayList<>();
	private final List<D> minOrderedIntervalsData = new ArrayList<>();

	private final List<Pair<C>> maxOrderedIntervals = new ArrayList<>();
	private final List<D> maxOrderedIntervalsData = new ArrayList<>();

	private final Comparator<Pair<C>> startIntervalComparator = (p1, p2) -> p1.max.compareTo(p2.min);
	private final Comparator<Pair<C>> endIntervalComparator = (p1, p2) -> p1.min.compareTo(p2.max);

	protected IntervalList(final Collection<I> intervals) {
		final Collection<I> input = intervals != null ? intervals : Collections.emptyList();
		final List<I> inputList = (input instanceof List) ? (List<I>) input : new ArrayList<>(input);

		Collections.sort(inputList, (i1, i2) -> i1.min.compareTo(i2.min));
		for (final I interval : inputList) {
			minOrderedIntervals.add(Pair.of(interval.min, interval.max));
			minOrderedIntervalsData.add(interval.data);
		}
		Collections.sort(inputList, (i1, i2) -> i1.max.compareTo(i2.max));
		for (final I interval : inputList) {
			maxOrderedIntervals.add(Pair.of(interval.min, interval.max));
			maxOrderedIntervalsData.add(interval.data);
		}
	}

	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalList<C, D, I> build(
			final Collection<I> intervals) {
		if (intervals == null || intervals.isEmpty())
			return emptyTree();

		return new IntervalList<C, D, I>(intervals);
	}

	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(
			final ISerializer<C> pointSerializer, final ISerializer<D> dataSerializer,
			final Constructor<I> constructor) {
		return new Serializer<>(pointSerializer, dataSerializer, constructor);
	}

	@SuppressWarnings("unchecked")
	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalList<C, D, I> emptyTree() {
		return EMPTY_TREE;
	}

	public int intervalCount() {
		return minOrderedIntervals.size();
	}

	public boolean isEmpty() {
		return minOrderedIntervals.isEmpty();
	}

	public C max() {
		if (isEmpty()) {
			throw new IllegalStateException();
		}

		return maxOrderedIntervals.get(intervalCount() - 1).max;
	}

	public C min() {
		if (isEmpty()) {
			throw new IllegalStateException();
		}

		return minOrderedIntervals.get(0).min;
	}

	public List<D> search(final Interval<C, D> searchInterval) {
		final Pair<C> searchIntervalPoints = Pair.of(searchInterval.min, searchInterval.max);
		int startIndex = findIntervalsWithMaxGreaterThanOrEqualToSearchIntervalMin(searchIntervalPoints);
		int endIndex = findIntervalsWithMinLessThanOrEqualToSearchIntervalMax(searchIntervalPoints);

		List<Pair<C>> intervals;
		List<D> intervalData;
		int start = 0, end = intervalCount() - 1;

		// pick the one that eliminates the most elements.
		if (startIndex > intervalCount() - endIndex) {
			intervals = maxOrderedIntervals;
			intervalData = maxOrderedIntervalsData;
			start = startIndex;
		} else {
			intervals = minOrderedIntervals;
			intervalData = minOrderedIntervalsData;
			end = endIndex;
		}

		// linear walk the remaining sub-list.
		final List<D> results = new ArrayList<>();
		for (int i = start; i <= end; i++) {
			Pair<C> interval = intervals.get(i);
			if (overlaps(interval, searchIntervalPoints)) {
				results.add(intervalData.get(i));
			}
		}

		return results;
	}

	public List<D> search(final C point) {
		return search(Interval.<C, D>create(point, point, null));
	}

	public Iterator<I> iterator() {
		final Iterator<Pair<C>> intervalPointsIterator = minOrderedIntervals.iterator();
		final Iterator<D> intervalsDataIterator = minOrderedIntervalsData.iterator();

		return new AbstractIterator<I>() {
			@Override
			protected I computeNext() {
				if (!intervalPointsIterator.hasNext()) {
					return endOfData();
				}

				final Pair<C> point = intervalPointsIterator.next();
				final D data = intervalsDataIterator.next();

				return (I) new Interval<C, D>(point.min, point.max, data);
			}
		};
	}

	@Override
	public String toString() {
		return "<" + Joiner.on(", ").join(this) + ">";
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof IntervalList)) {
			return false;
		}

		@SuppressWarnings("rawtypes")
		final IntervalList that = (IntervalList) o;
		return Iterators.elementsEqual(iterator(), that.iterator());
	}

	@Override
	public final int hashCode() {
		int result = 0;
		for (final Interval<C, D> interval : this)
			result = 31 * result + interval.hashCode();
		return result;
	}

	// Assuming search interval is si, this method returns the starting interval
	// index (inclusive) i, such that, i.max >= si.min
	private int findIntervalsWithMaxGreaterThanOrEqualToSearchIntervalMin(final Pair<C> searchIntervalPoints) {
		int index = Collections.binarySearch(maxOrderedIntervals, searchIntervalPoints, startIntervalComparator);

		// searchInterval.min not found, the the insertion point is our start index
		// (inclusion).
		if (index < 0) {
			return Math.abs(index) - 1;
		}

		// searchInterval.max is found in which case we need to find all the elements.
		while (index >= 0 && maxOrderedIntervals.get(index).max.equals(searchIntervalPoints.min)) {
			index--;
		}
		return index + 1;
	}

	// Assuming search interval is si, this method returns the ending interval
	// index (inclusive) i, such that, i.min <= si.max.
	private int findIntervalsWithMinLessThanOrEqualToSearchIntervalMax(final Pair<C> searchIntervalPoints) {
		int index = Collections.binarySearch(minOrderedIntervals, searchIntervalPoints, endIntervalComparator);

		// searchInterval.max not found, the the insertion point is our end index
		// (exclusion).
		if (index < 0) {
			index = Math.abs(index) - 1;
		} else {
			// searchInterval.max is found in which case we need to find all the elements.
			while (index < intervalCount() && minOrderedIntervals.get(index).min.equals(searchIntervalPoints.max)) {
				index++;
			}
		}

		return index - 1;
	}

	private boolean overlaps(Pair<C> i1, Pair<C> i2) {
		return !((i1.max.compareTo(i2.min) < 0 || i1.min.compareTo(i2.max) > 0));
	}

	public static class Serializer<C extends Comparable<? super C>, D, I extends Interval<C, D>>
			implements IVersionedSerializer<IntervalList<C, D, I>> {
		private final ISerializer<C> pointSerializer;
		private final ISerializer<D> dataSerializer;
		private final Constructor<I> constructor;

		private Serializer(final ISerializer<C> pointSerializer, final ISerializer<D> dataSerializer,
				final Constructor<I> constructor) {
			this.pointSerializer = pointSerializer;
			this.dataSerializer = dataSerializer;
			this.constructor = constructor;
		}

		public void serialize(final IntervalList<C, D, I> it, final DataOutputPlus out, final int version)
				throws IOException {
			out.writeInt(it.intervalCount());
			for (final Interval<C, D> interval : it) {
				pointSerializer.serialize(interval.min, out);
				pointSerializer.serialize(interval.max, out);
				dataSerializer.serialize(interval.data, out);
			}
		}

		/**
		 * Deserialize an IntervalTree whose keys use the natural ordering. Use
		 * deserialize(DataInput, int, Comparator) instead if the interval tree is to
		 * use a custom comparator, as the comparator is *not* serialized.
		 */
		public IntervalList<C, D, I> deserialize(final DataInputPlus in, final int version) throws IOException {
			return deserialize(in, version, null);
		}

		public IntervalList<C, D, I> deserialize(final DataInputPlus in, final int version,
				final Comparator<C> comparator) throws IOException {
			try {
				final int count = in.readInt();
				final List<I> intervals = new ArrayList<I>(count);
				for (int i = 0; i < count; i++) {
					final C min = pointSerializer.deserialize(in);
					final C max = pointSerializer.deserialize(in);
					final D data = dataSerializer.deserialize(in);
					intervals.add(constructor.newInstance(min, max, data));
				}
				return new IntervalList<C, D, I>(intervals);
			} catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		public long serializedSize(final IntervalList<C, D, I> it, final int version) {
			long size = TypeSizes.sizeof(0);
			for (final Interval<C, D> interval : it) {
				size += pointSerializer.serializedSize(interval.min);
				size += pointSerializer.serializedSize(interval.max);
				size += dataSerializer.serializedSize(interval.data);
			}
			return size;
		}
	}

	private static class Pair<C> {
		public final C min;
		public final C max;

		private Pair(final C min, final C max) {
			this.min = min;
			this.max = max;
		}

		static <C> Pair<C> of(final C min, final C max) {
			return new Pair<C>(min, max);
		}
	}
}
