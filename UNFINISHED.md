### D1. Partial log unavailability (the piece you're deferring)

 Current state, so it's recoverable later:
 - Cleanup.isUnavailable (Cleanup.java:229-233) — any(LOG_UNAVAILABLE) or all(LOG_INCOMPLETE) && saveStatus < Stable, with your TODO (required): how does this interact with lost ownership?.
 - SafeCommandStore.registerTransitiveDeps (~:565) — per-range subtraction via redundantBefore.removeLogUnavailableOrIncomplete(txnId, tmp), with TODO (required): if we only part-filter we're still going to have problems, as we won't be able to
   update the transaction.
 - RedundantBefore.Bounds.depBound — folds LOG_INCOMPLETE/LOG_UNAVAILABLE into the dep bound when UNREADY is ahead of the applied bound; this is what keeps deps sound after a restart, when the in-memory refuses map is gone.
 - MapReduceCommandStores.Refuse{NONE,DEPS,ALL} + per-message abort() — the in-memory, coarse (whole-scope) refusal, only alive between unsafeRefuseRequests and unsafeAcceptRequests.

 These four must agree on one predicate. Concretely the questions to settle:
 1. Is any ever right for LOG_UNAVAILABLE? The dangerous arm isn't FULL (throwing is merely unavailability) but non-FULL, where logUnavailable(...) returns ERASE — a compaction decision that discards the record for ranges whose log is intact.
    If nothing else changes, splitting just that arm (throw on any, erase only on all) is strictly safer and is a one-line intermediate.
 2. Lost ownership / retired ranges break all. Ranges that are WAS_OWNED or locally retired never carry LOG_INCOMPLETE, so all(...) is false and we don't refuse even though the owned remainder is incomplete. Your "treat pre-log-point ranges as
    locally-retired-like" idea is exactly the missing normalisation, and note the patch already does the dual of it in SafeCommandStore.refuses(participants) (removeLocallyRetired(participants) before folding). A shared helper — "the
    participants we must have a complete log for" = participants − retired − wasOwned − logIncomplete/Unavailable — would let Cleanup, refuses() and registerTransitive share one definition instead of three.
 3. Recovery cannot answer partially. BeginRecovery refuses on refuses.max != NONE, which only covers the in-memory window, not the post-restart window. There is no wire representation for "valid only for a sub-range" — but ReadOk.unavailable
    already is exactly that for reads, so a RecoverNack{unavailable} (and the analogous field on the deps replies) is the smallest precedent-following addition; it would also let Commit/Apply stop depending on the coarse Refuse.MinMax.
 4. Future distinction you mentioned: if you do split further, the axis that matters to consumers looks like "records absent below the bound" (incomplete) vs "records possibly wrong below the bound" (corrupted) vs "records absent but decisions
    recoverable from peers". Worth writing that down in BootstrapReason/RedundantStatus.Property javadoc before adding a third status, since the persisted bit space is nearly full (see N9).

 ### D2. Empty / short DurabilityResults

 Now benign-ish but still wrong: DurabilityService.submit's canUseDurableBefore && isAlreadyMet shortcut calls reportSuccess() while success == DurabilityResults.EMPTY, so markBootstrapping marks nothing and the attempt completes with missing
 == valid → onFailedBootstrap → retry. Since each retry re-derives a fresh maxConflict and can be satisfied by durableBefore again, an idle range can retry indefinitely, with the unhelpful "Unknown failure" from complete()'s fail runnable.
 Options: (a) populate DurabilityResults.of(ranges, request.min, <nodes>) on the shortcut path; (b) make the shortcut ineligible when the requester needs per-range bounds/readable (a flag on the request); (c) fail prepareToBootstrap explicitly
 if byTxnId() doesn't cover the requested ranges, so the diagnostic names the cause. Also sync(...)/close(...) still return success(null) for empty ranges — an empty DurabilityResults would be safer for every caller that dereferences the value.
 Worth adding an Invariants.require(success.byTxnId() covers commitRanges) at the markBootstrapping site so this class of shortfall is loud rather than a retry loop.

