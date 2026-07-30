---- MODULE Notify ----
\* Three tasks sharing one command entry and two key entries, with mixed regions
\* and mixed sync-ness: enough for a task to be demoted on the txnId while holding
\* key positions, which is the state the counters have to survive.
EXTENDS AccordNotify
MCTaskTxns   == <<{"c1"}, {"c1"}, {}>>
MCTaskKeys   == <<{"k1","k2"}, {"k1"}, {"k1","k2"}>>
MCTaskRegion == <<"ORD", "FIFO", "ORD">>
MCTaskSync   == <<TRUE, FALSE, FALSE>>
====
