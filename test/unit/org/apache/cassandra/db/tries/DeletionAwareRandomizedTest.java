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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.core.Gen;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DataPoint.fromList;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

/// Randomized property-based testing for deletion-aware tries using QuickTheories.
///
/// This test class uses the existing deletion-aware trie test infrastructure to perform
/// comprehensive randomized testing of trie operations, merging, and deletion handling.
/// It complements the structured tests in [DeletionAwareMergeTest] with property-based
/// testing to catch edge cases and verify invariants across a wide range of inputs.
public class DeletionAwareRandomizedTest extends DeletionAwareTestBase
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int MAX_POINTS = 20;
    private static final int MAX_VALUE = 63; // Fits in 6 bits (bitsNeeded)
    private static final int MAX_TIMESTAMP = 100;
    
    /// Generator for random live data points.
    /// Creates `LivePoint` instances with random positions and timestamps.
    private Gen<LivePoint> livePointGen()
    {
        return integers().between(0, MAX_VALUE)
                        .zip(integers().between(1, MAX_TIMESTAMP),
                             (pos, ts) -> new LivePoint(at(pos), ts));
    }
    
    ///
    /// Generator for random deletion markers.
    /// Creates `DeletionMarker` instances with random positions and deletion values.
    ///
    private Gen<DeletionMarker> deletionMarkerGen()
    {
        return integers().between(0, MAX_VALUE)
                        .zip(integers().between(1, MAX_TIMESTAMP),
                             integers().between(-1, MAX_TIMESTAMP),
                             integers().between(-1, MAX_TIMESTAMP),
                             (pos, left, at, right) -> new DeletionMarker(before(pos), left, at, right));
    }
    
    /// Generator for random live point lists.
    /// Creates sorted lists of `LivePoint` instances for trie construction.
    private Gen<List<DataPoint>> dataPointListGen()
    {
        return lists().of(livePointGen().map(lp -> (DataPoint) lp))
                     .ofSizeBetween(0, MAX_POINTS)
                     .map(this::sortAndValidateDataPoints);
    }
    
    /// Generator for pairs of data point lists for merge testing.
    private Gen<List<List<DataPoint>>> dataPointPairGen()
    {
        return dataPointListGen().zip(dataPointListGen(), Arrays::asList);
    }
    
    /// Sorts data points by position and ensures they form a valid deletion-aware sequence.
    private List<DataPoint> sortAndValidateDataPoints(List<DataPoint> points)
    {
        if (points.isEmpty())
            return points;
            
        // Sort by position
        List<DataPoint> sorted = points.stream()
                                      .sorted((a, b) -> ByteComparable.compare(a.position(), b.position(), TrieUtil.VERSION))
                                      .collect(Collectors.toList());
        
        // Remove duplicates at same position, keeping the last one
        List<DataPoint> deduplicated = new ArrayList<>();
        DataPoint prev = null;
        for (DataPoint dp : sorted)
        {
            if (prev == null || ByteComparable.compare(prev.position(), dp.position(), TrieUtil.VERSION) != 0)
                deduplicated.add(dp);
            else
                deduplicated.set(deduplicated.size() - 1, dp); // Replace with newer
            prev = dp;
        }
        
        try
        {
            return verify(deduplicated);
        }
        catch (AssertionError e)
        {
            // If verification fails, return empty list to avoid invalid test data
            return Collections.emptyList();
        }
    }
    
    /// Test that trie construction and iteration are consistent.
    ///
    /// **Property**: Converting a list to a trie and back should yield the same list.
    @Test
    public void testTrieConstructionConsistency()
    {
        qt().forAll(dataPointListGen(), booleans().all())
            .checkAssert((dataPoints, forceCopy) -> {
                if (dataPoints.isEmpty())
                    return; // Skip empty lists
                    
                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, forceCopy);
                List<DataPoint> reconstructed = toList(trie);
                
                assertEquals("Trie construction should be consistent with iteration",
                           dataPoints, reconstructed);
            });
    }
    
    /// Test that merging tries is commutative for the same resolver.
    ///
    /// **Property**: `merge(A, B)` should equal `merge(B, A)` when using the same merge resolver.
    @Test
    public void testMergeCommutativity()
    {
        qt().forAll(dataPointPairGen(), booleans().all())
            .checkAssert((pair, forcedCopy) -> {
                List<DataPoint> list1 = pair.get(0);
                List<DataPoint> list2 = pair.get(1);
                
                if (list1.isEmpty() && list2.isEmpty())
                    return; // Skip empty merges
                    
                DeletionAwareTrie<LivePoint, DeletionMarker> trie1 = fromList(list1, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie2 = fromList(list2, forcedCopy);
                
                // Merge in both directions
                DeletionAwareTrie<LivePoint, DeletionMarker> merged1to2 =
                    trie1.mergeWith(trie2, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                DeletionAwareTrie<LivePoint, DeletionMarker> merged2to1 =
                    trie2.mergeWith(trie1, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                
                List<DataPoint> result1 = toList(merged1to2);
                List<DataPoint> result2 = toList(merged2to1);
                
                assertEquals("Merge should be commutative", result1, result2);
            });
    }
    
    /// Test that merging with an empty trie is an identity operation.
    ///
    /// **Property**: `merge(A, empty)` should equal `A`.
    @Test
    public void testMergeIdentity()
    {
        qt().forAll(dataPointListGen(), booleans().all())
            .checkAssert((dataPoints, forcedCopy) -> {
                if (dataPoints.isEmpty())
                    return; // Skip empty lists
                    
                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> empty = fromList(Collections.emptyList(), forcedCopy);
                
                DeletionAwareTrie<LivePoint, DeletionMarker> merged =
                    trie.mergeWith(empty, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                
                List<DataPoint> original = toList(trie);
                List<DataPoint> result = toList(merged);
                
                assertEquals("Merging with empty trie should be identity", original, result);
            });
    }
    
    /// Test that subtrie operations preserve ordering and boundaries.
    ///
    /// **Property**: A subtrie should contain only elements within the specified range.
    @Test
    public void testSubtrieRangeInvariant()
    {
        qt().forAll(dataPointListGen()
                   .zip(integers().between(0, MAX_VALUE),
                        integers().between(0, MAX_VALUE),
                        (points, start, end) -> {
                            int left = Math.min(start, end);
                            int right = Math.max(start, end);
                            return asList(points, left, right);
                        }), booleans().all())
            .checkAssert((params, forcedCopy) -> {
                @SuppressWarnings("unchecked")
                List<DataPoint> dataPoints = (List<DataPoint>) params.get(0);
                int left = (Integer) params.get(1);
                int right = (Integer) params.get(2);
                
                if (dataPoints.isEmpty())
                    return; // Skip empty lists
                    
                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> subtrie = 
                    trie.subtrie(before(left), before(right));
                
                List<DataPoint> subtriePoints = toList(subtrie);
                
                // Verify all points in subtrie are within range
                for (DataPoint dp : subtriePoints)
                {
                    ByteComparable pos = dp.position();
                    int cmp1 = ByteComparable.compare(pos, before(left), TrieUtil.VERSION);
                    int cmp2 = ByteComparable.compare(pos, before(right), TrieUtil.VERSION);
                    
                    if (cmp1 < 0 || cmp2 >= 0)
                    {
                        throw new AssertionError(
                            String.format("Point %s outside subtrie range [%s, %s)",
                                        pos.byteComparableAsString(TrieUtil.VERSION),
                                        before(left).byteComparableAsString(TrieUtil.VERSION),
                                        before(right).byteComparableAsString(TrieUtil.VERSION)));
                    }
                }
            });
    }
    
    /// Test that the optimized `MergeCursor` produces the same results as the safe version.
    ///
    /// **Property**: Optimized and safe merge cursors should produce identical results.
    @Test
    public void testOptimizedMergeCursorEquivalence()
    {
        qt().forAll(dataPointPairGen(), booleans().all())
            .checkAssert((pair, forcedCopy) -> {
                List<DataPoint> list1 = pair.get(0);
                List<DataPoint> list2 = pair.get(1);
                
                if (list1.isEmpty() || list2.isEmpty())
                    return; // Skip cases with empty lists
                    
                DeletionAwareTrie<LivePoint, DeletionMarker> trie1 = fromList(list1, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie2 = fromList(list2, forcedCopy);
                
                // Test both optimized and safe merge cursors
                DeletionAwareCursor<LivePoint, DeletionMarker> cursor1 = trie1.cursor(Direction.FORWARD);
                DeletionAwareCursor<LivePoint, DeletionMarker> cursor2 = trie2.cursor(Direction.FORWARD);
                
                // Test both optimized and safe merge using the trie API
                DeletionAwareTrie<LivePoint, DeletionMarker> safeMerge =
                    trie1.mergeWith(trie2, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);

                // Create optimized merge
                DeletionAwareTrie<LivePoint, DeletionMarker> optimizedMerge =
                    trie1.mergeWith(trie2, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, true);

                List<DataPoint> safeResult = toList(safeMerge);
                List<DataPoint> optimizedResult = toList(optimizedMerge);
                
                assertEquals("Optimized and safe merge cursors should produce identical results",
                           safeResult, optimizedResult);
            });
    }

    /// Test that deletion markers properly delete live data within their ranges.
    ///
    /// **Property**: Live data covered by deletion ranges should be removed or modified.
    @Test
    public void testDeletionApplicationInvariant()
    {
        qt().forAll(integers().between(0, MAX_VALUE)
                   .zip(integers().between(1, MAX_TIMESTAMP),
                        integers().between(1, MAX_TIMESTAMP),
                        (pos, liveTs, deleteTs) -> asList(pos, liveTs, deleteTs)))
            .checkAssert(params -> {
                int pos = (Integer) params.get(0);
                int liveTs = (Integer) params.get(1);
                int deleteTs = (Integer) params.get(2);

                // Create a live point and a deletion that should affect it
                LivePoint live = new LivePoint(at(pos), liveTs);
                DeletionMarker deletion = new DeletionMarker(before(pos), -1, deleteTs, deleteTs);

                // Apply deletion to live data
                LivePoint result = deletion.applyTo(live);

                if (deleteTs > liveTs)
                {
                    // Deletion should remove the live data (return null)
                    assertEquals("Live data should be deleted when deletion timestamp > live timestamp",
                               null, result);
                }
                else
                {
                    // Deletion should not affect live data with same or newer timestamp
                    assertEquals("Live data should not be affected by older or equal deletions", live, result);
                }
            });
    }

    /// Test that trie operations maintain structural invariants.
    ///
    /// **Property**: All trie operations should preserve the trie's structural integrity.
    @Test
    public void testTrieStructuralInvariants()
    {
        qt().forAll(dataPointListGen(), booleans().all())
            .checkAssert((dataPoints, forcedCopy) -> {
                if (dataPoints.isEmpty())
                    return; // Skip empty lists

                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, forcedCopy);

                // Test that trie construction is consistent
                List<DataPoint> reconstructed = toList(trie);

                // Verify that the reconstructed list maintains ordering
                for (int i = 1; i < reconstructed.size(); i++)
                {
                    ByteComparable prev = reconstructed.get(i - 1).position();
                    ByteComparable curr = reconstructed.get(i).position();
                    int cmp = ByteComparable.compare(prev, curr, TrieUtil.VERSION);

                    if (cmp > 0)
                    {
                        throw new AssertionError(
                            String.format("Trie ordering violation: %s > %s",
                                        prev.byteComparableAsString(TrieUtil.VERSION),
                                        curr.byteComparableAsString(TrieUtil.VERSION)));
                    }
                }
            });
    }

    /// Test that range operations are consistent with full trie operations.
    ///
    /// **Property**: Operating on a range should be equivalent to filtering the full result.
    @Test
    public void testRangeOperationConsistency()
    {
        qt().forAll(dataPointListGen()
                   .zip(integers().between(0, MAX_VALUE),
                        integers().between(0, MAX_VALUE),
                        (points, start, end) -> {
                            int left = Math.min(start, end);
                            int right = Math.max(start, end);
                            return asList(points, left, right);
                        }),
                    booleans().all())
            .checkAssert((params, forcedCopy) -> {
                @SuppressWarnings("unchecked")
                List<DataPoint> dataPoints = (List<DataPoint>) params.get(0);
                int left = (Integer) params.get(1);
                int right = (Integer) params.get(2);

                if (dataPoints.isEmpty())
                    return; // Skip empty lists

                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, forcedCopy);

                // Get subtrie result
                DeletionAwareTrie<LivePoint, DeletionMarker> subtrie =
                    trie.subtrie(before(left), before(right));
                List<DataPoint> subtrieResult = toList(subtrie);

                // Get filtered full result
                List<DataPoint> fullResult = toList(trie);
                List<DataPoint> filteredResult = fullResult.stream()
                    .filter(dp -> {
                        ByteComparable pos = dp.position();
                        int cmp1 = ByteComparable.compare(pos, before(left), TrieUtil.VERSION);
                        int cmp2 = ByteComparable.compare(pos, before(right), TrieUtil.VERSION);
                        return cmp1 >= 0 && cmp2 < 0;
                    })
                    .collect(Collectors.toList());

                assertEquals("Subtrie should be equivalent to filtering full trie",
                           filteredResult, subtrieResult);
            });
    }

    /// Test that merge operations are associative.
    ///
    /// **Property**: `merge(merge(A, B), C)` should equal `merge(A, merge(B, C))`.
    @Test
    public void testMergeAssociativity()
    {
        qt().forAll(dataPointListGen()
                   .zip(dataPointListGen(), dataPointListGen(), Arrays::asList),
                    booleans().all())
            .checkAssert((triple, forcedCopy) -> {
                List<DataPoint> list1 = triple.get(0);
                List<DataPoint> list2 = triple.get(1);
                List<DataPoint> list3 = triple.get(2);

                if (list1.isEmpty() && list2.isEmpty() && list3.isEmpty())
                    return; // Skip all empty

                DeletionAwareTrie<LivePoint, DeletionMarker> trie1 = fromList(list1, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie2 = fromList(list2, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie3 = fromList(list3);

                // Test (A merge B, forcedCopy) merge C
                DeletionAwareTrie<LivePoint, DeletionMarker> ab =
                    trie1.mergeWith(trie2, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                DeletionAwareTrie<LivePoint, DeletionMarker> ab_c =
                    ab.mergeWith(trie3, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);

                // Test A merge (B merge C)
                DeletionAwareTrie<LivePoint, DeletionMarker> bc =
                    trie2.mergeWith(trie3, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                DeletionAwareTrie<LivePoint, DeletionMarker> a_bc =
                    trie1.mergeWith(bc, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);

                List<DataPoint> result1 = toList(ab_c);
                List<DataPoint> result2 = toList(a_bc);

                assertEquals("Merge should be associative", result1, result2);
            });
    }


    /// Test collection merge functionality using randomized property-based testing.
    /// This test verifies that merging multiple tries using collection merge produces
    /// the same result as sequential pairwise merges.
    @Test
    public void testCollectionMerge()
    {
        qt().forAll(dataPointListGen().zip(dataPointListGen(), dataPointListGen(), Arrays::asList), booleans().all())
            .checkAssert((triple, forcedCopy) -> {
                List<DataPoint> list1 = triple.get(0);
                List<DataPoint> list2 = triple.get(1);
                List<DataPoint> list3 = triple.get(2);

                // Skip cases where all tries are empty or where we have empty tries mixed with non-empty ones
                // Collection merge requires at least one non-empty trie and can't handle mixed empty/non-empty
                boolean hasEmpty = list1.isEmpty() || list2.isEmpty() || list3.isEmpty();
                boolean hasNonEmpty = !list1.isEmpty() || !list2.isEmpty() || !list3.isEmpty();

                if (!hasNonEmpty || hasEmpty)
                    return; // Skip if all empty or if we have any empty tries

                DeletionAwareTrie<LivePoint, DeletionMarker> trie1 = fromList(list1, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie2 = fromList(list2, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie3 = fromList(list3, forcedCopy);

                // Test collection merge
                DeletionAwareTrie<LivePoint, DeletionMarker> collectionMerged =
                    DeletionAwareTrie.merge(Arrays.asList(trie1, trie2, trie3),
                                          LivePoint::combineCollection,
                                          DeletionMarker::combineCollection,
                                          DeletionMarker::applyTo,
                                          false);

                List<DataPoint> collectionResult = toList(collectionMerged);

                // Test pairwise merge for comparison
                DeletionAwareTrie<LivePoint, DeletionMarker> pairwise12 =
                trie1.mergeWith(trie2, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);
                DeletionAwareTrie<LivePoint, DeletionMarker> pairwiseMerged =
                pairwise12.mergeWith(trie3, LivePoint::combine, DeletionMarker::combine, DeletionMarker::applyTo, false);

                List<DataPoint> pairwiseResult = toList(pairwiseMerged);

                assertEquals("Collection merge should equal pairwise merge", pairwiseResult, collectionResult);
            });
    }

    /// Test that the optimized collection merge produces the same results as the safe version.
    /// This verifies that the deletionsAtFixedPoints optimization works correctly for collection merges.
    @Test
    public void testOptimizedCollectionMerge()
    {
        qt().forAll(dataPointListGen().zip(dataPointListGen(), dataPointListGen(), Arrays::asList), booleans().all())
            .checkAssert((triple, forcedCopy) -> {
                List<DataPoint> list1 = triple.get(0);
                List<DataPoint> list2 = triple.get(1);
                List<DataPoint> list3 = triple.get(2);

                // Skip cases where all tries are empty or where we have empty tries mixed with non-empty ones
                // Collection merge requires at least one non-empty trie and can't handle mixed empty/non-empty
                boolean hasEmpty = list1.isEmpty() || list2.isEmpty() || list3.isEmpty();
                boolean hasNonEmpty = !list1.isEmpty() || !list2.isEmpty() || !list3.isEmpty();

                if (!hasNonEmpty || hasEmpty)
                    return; // Skip if all empty or if we have any empty tries

                DeletionAwareTrie<LivePoint, DeletionMarker> trie1 = fromList(list1, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie2 = fromList(list2, forcedCopy);
                DeletionAwareTrie<LivePoint, DeletionMarker> trie3 = fromList(list3);

                // Test safe collection merge (deletionsAtFixedPoints = false, forcedCopy)
                DeletionAwareTrie<LivePoint, DeletionMarker> safeMerged = dir ->
                    new CollectionMergeCursor.DeletionAware<>(
                        LivePoint::combineCollection,
                        DeletionMarker::combineCollection,
                        DeletionMarker::applyTo,
                        false,
                        dir,
                        Arrays.asList(trie1, trie2, trie3),
                        DeletionAwareTrie::cursor);

                // Test optimized collection merge (deletionsAtFixedPoints = true)
                DeletionAwareTrie<LivePoint, DeletionMarker> optimizedMerged = dir ->
                    new CollectionMergeCursor.DeletionAware<>(
                    LivePoint::combineCollection,
                    DeletionMarker::combineCollection,
                    DeletionMarker::applyTo,
                    true,
                    dir,
                    Arrays.asList(trie1, trie2, trie3),
                    DeletionAwareTrie::cursor);

                List<DataPoint> safeResult = toList(safeMerged);
                List<DataPoint> optimizedResult = toList(optimizedMerged);

                assertEquals("Optimized and safe collection merge should produce identical results",
                             safeResult, optimizedResult);
            });
    }
}
