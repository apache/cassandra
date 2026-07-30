---- MODULE Baseline ----
\* topology hold-vs-consequence under profile baseline; see ../matrix.py
EXTENDS AccordExec
MCTaskTxns   == <<{"c1"}, {"c1"}>>
MCTaskKeys   == <<{"k1", "k2"}, {"k1"}>>
MCTaskParent == <<0, 1>>
====
