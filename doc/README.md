Apache Cassandra documentation directory
========================================

This directory contains the documentation maintained in-tree for Apache
Cassandra. This directory contains the following documents:
- The source of the official Cassandra documentation, in the `source/`
  subdirectory. See below for more details on how to edit/build that
  documentation.
- The specification(s) for the supported versions of native transport protocol.
- Additional documentation on the SASI implementation (`SASI.md`). TODO: we
  should probably move the first half of that documentation to the general
  documentation, and the implementation explanation parts into the wiki.


Official documentation
----------------------

The source for the official documentation for Apache Cassandra can be found in
the `source` subdirectory. The documentation uses [sphinx](http://www.sphinx-doc.org/)
and is thus written in [reStructuredText](http://docutils.sourceforge.net/rst.html).

To build the HTML documentation, you will need to first install sphinx and the
[sphinx ReadTheDocs theme](the https://pypi.python.org/pypi/sphinx_rtd_theme), which
on unix you can do with:
```
pip install sphinx sphinx_rtd_theme
```

The documentation can then be built from this directory by calling `make html`
(or `make.bat html` on windows). Alternatively, the top-level `ant gen-doc`
target can be used.
