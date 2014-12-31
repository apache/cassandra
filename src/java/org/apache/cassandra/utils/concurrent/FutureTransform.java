package org.apache.cassandra.utils.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class FutureTransform {
    /**
     * We'd like to utilize existing CompletableFuture.allOf(). Unfortunately,
     * it has two minor drawbacks - it takes vararg instead of Collection and
     * doesn't return a future of aggregated results but Void instead. By
     * aggregated results I mean: if we provide List<CompletableFuture<Double>>,
     * it should return CompletableFuture<List<Double>>, not
     * CompletableFuture<Void>! Luckily it's easy to fix with a bit of glue
     * code.
     */
    public static <T> CompletableFuture<List<T>> allAsList(
            Collection<? extends CompletableFuture<T>> futures) {
        // the trick is to use existing allOf() but when allDoneFuture completes
        // (which means all/ underlying futures are done),
        // simply iterate overall futures and join() (blocking wait) on each.
        // However this call is guaranteed not to block because
        // by now all futures completed!
        CompletableFuture<Void> allDoneFuture = CompletableFuture.allOf(futures
                .toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApply(v -> {
            return futures.stream().map(CompletableFuture::join)
                    .collect(Collectors.toList());
        });
    }
}
