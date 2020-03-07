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
import java.util.stream.Collectors;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

public class IntervalTree<C extends Comparable<? super C>, D, I extends Interval<C, D>> implements Iterable<I> {
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static final IntervalTree EMPTY_TREE = new IntervalTree(Collections.emptyList());

	private final List<I> minOrderedIntervals;
	private final List<I> maxOrderedIntervals;

	// TODO Look for a better variable name?
	private final Comparator<Interval<C, D>> startIntervalComparator = (i1, i2) -> i1.max.compareTo(i2.min);
	private final Comparator<Interval<C, D>> endIntervalComparator = (i1, i2) -> i1.min.compareTo(i2.max);

	protected IntervalTree(Collection<I> intervals) {
		Collection<I> input = intervals != null ? intervals : Collections.emptyList();
		this.minOrderedIntervals = input.stream().sorted((i1, i2) -> i1.min.compareTo(i2.min))
				.collect(Collectors.toList());
		this.maxOrderedIntervals = input.stream().sorted((i1, i2) -> i1.max.compareTo(i2.max))
				.collect(Collectors.toList());
	}

	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> build(
			Collection<I> intervals) {
		if (intervals == null || intervals.isEmpty())
			return emptyTree();

		return new IntervalTree<C, D, I>(intervals);
	}

	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> Serializer<C, D, I> serializer(
			ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor) {
		return new Serializer<>(pointSerializer, dataSerializer, constructor);
	}

	@SuppressWarnings("unchecked")
	public static <C extends Comparable<? super C>, D, I extends Interval<C, D>> IntervalTree<C, D, I> emptyTree() {
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

		return maxOrderedIntervals.get(maxOrderedIntervals.size() - 1).max;
	}

	public C min() {
		if (isEmpty()) {
			throw new IllegalStateException();
		}

		return minOrderedIntervals.get(0).min;
	}

	public List<D> search(Interval<C, D> searchInterval) {
		int startIndex = findIntervalsWithMaxGreaterThanOrEqualToSearchIntervalMin(searchInterval);
		int endIndex = findIntervalsWithMinLessThanOrEqualToSearchIntervalMax(searchInterval);

		List<I> intervals;
		int start = 0, end = minOrderedIntervals.size() - 1;

		// pick the one that eliminates the most elements.
		if (startIndex > minOrderedIntervals.size() - endIndex) {
			intervals = maxOrderedIntervals;
			start = startIndex;
		} else {
			intervals = minOrderedIntervals;
			end = endIndex;
		}

		// linear walk the remaining sub-list.
		List<D> results = new ArrayList<>();
		for (int i = start; i <= end; i++) {
			I interval = intervals.get(i);
			if (overlaps(interval, searchInterval)) {
				results.add(interval.data);
			}
		}

		return results;
	}

	public List<D> search(C point) {
		return search(Interval.<C, D>create(point, point, null));
	}

	public Iterator<I> iterator() {
		return minOrderedIntervals.iterator();
	}

	@Override
	public String toString() {
		return "<" + Joiner.on(", ").join(this) + ">";
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntervalTree)) {
			return false;
		}

		@SuppressWarnings("rawtypes")
		IntervalTree that = (IntervalTree) o;
		return Iterators.elementsEqual(iterator(), that.iterator());
	}

	@Override
	public final int hashCode() {
		int result = 0;
		for (Interval<C, D> interval : this)
			result = 31 * result + interval.hashCode();
		return result;
	}

	// TODO FIXME watch out for edge cases.
	// Assuming search interval is si, this method returns the starting interval
	// index (inclusive) i, such that, i.max >= si.min
	private int findIntervalsWithMaxGreaterThanOrEqualToSearchIntervalMin(Interval<C, D> searchInterval) {
		int index = Collections.binarySearch(maxOrderedIntervals, searchInterval, startIntervalComparator);

		// searchInterval.min not found, the the insertion point is our start index
		// (inclusion).
		if (index < 0) {
			return Math.abs(index) - 1;
		}

		// searchInterval.max is found in which case we need to find all the elements.
		while (index >= 0 && maxOrderedIntervals.get(index).max.equals(searchInterval.min)) {
			index--;
		}
		return index + 1;
	}

	// TODO FIXME watch out for edge cases.
	// Assuming search interval is si, this method returns the ending interval
	// index (inclusive) i, such that, i.min <= si.max.
	private int findIntervalsWithMinLessThanOrEqualToSearchIntervalMax(Interval<C, D> searchInterval) {
		int index = Collections.binarySearch(minOrderedIntervals, searchInterval, endIntervalComparator);

		// searchInterval.max not found, the the insertion point is our end index
		// (exclusion).
		if (index < 0) {
			index = Math.abs(index) - 1;
		} else {
			// searchInterval.max is found in which case we need to find all the elements.
			while (index < minOrderedIntervals.size() && minOrderedIntervals.get(index).min.equals(searchInterval.max)) {
				index++;
			}
		}

		return index - 1;
	}

	private boolean overlaps(Interval<C, D> i1, Interval<C, D> i2) {
		return !((i1.max.compareTo(i2.min) < 0 || i1.min.compareTo(i2.max) > 0));
	}

	public static class Serializer<C extends Comparable<? super C>, D, I extends Interval<C, D>>
			implements IVersionedSerializer<IntervalTree<C, D, I>> {
		private final ISerializer<C> pointSerializer;
		private final ISerializer<D> dataSerializer;
		private final Constructor<I> constructor;

		private Serializer(ISerializer<C> pointSerializer, ISerializer<D> dataSerializer, Constructor<I> constructor) {
			this.pointSerializer = pointSerializer;
			this.dataSerializer = dataSerializer;
			this.constructor = constructor;
		}

		public void serialize(IntervalTree<C, D, I> it, DataOutputPlus out, int version) throws IOException {
			out.writeInt(it.intervalCount());
			for (Interval<C, D> interval : it) {
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
		public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version) throws IOException {
			return deserialize(in, version, null);
		}

		public IntervalTree<C, D, I> deserialize(DataInputPlus in, int version, Comparator<C> comparator)
				throws IOException {
			try {
				int count = in.readInt();
				List<I> intervals = new ArrayList<I>(count);
				for (int i = 0; i < count; i++) {
					C min = pointSerializer.deserialize(in);
					C max = pointSerializer.deserialize(in);
					D data = dataSerializer.deserialize(in);
					intervals.add(constructor.newInstance(min, max, data));
				}
				return new IntervalTree<C, D, I>(intervals);
			} catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}

		public long serializedSize(IntervalTree<C, D, I> it, int version) {
			long size = TypeSizes.sizeof(0);
			for (Interval<C, D> interval : it) {
				size += pointSerializer.serializedSize(interval.min);
				size += pointSerializer.serializedSize(interval.max);
				size += dataSerializer.serializedSize(interval.data);
			}
			return size;
		}
	}
}
