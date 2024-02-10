[![tests](https://github.com/cowsql/raft/actions/workflows/tests.yml/badge.svg)](https://github.com/cowsql/raft/actions/workflows/tests.yml) [![codecov](https://codecov.io/gh/cowsql/raft/branch/main/graph/badge.svg)](https://codecov.io/gh/cowsql/raft) [![Documentation Status](https://readthedocs.org/projects/raft/badge/?version=latest)](https://raft.readthedocs.io/en/latest/?badge=latest) [![Coverity](https://scan.coverity.com/projects/28929/badge.svg)](https://scan.coverity.com/projects/cowsql-raft)

Production grade asynchronous C implementation of the Raft consensus protocol.

Documentation
-------------

See [readthedocs](https://raft.readthedocs.io/) for the full documentation.

Building
---------

```bash
autoreconf -i
./configure
make
```

History
-------

This library is a fork of [Canonical's](https://github.com/canonical/raft) Raft
implementation, which was originally written by this library's author
[himself](https://github.com/canonical/raft/commits?author=freeekanayaka) while
working at Canonical.

It is a **fully compatible drop-in replacement** of Canonical's version, at
least up to v0.18.0.

License
-------

This raft C library is released under a slightly modified version of LGPLv3,
that includes a copyright exception letting users to statically link the library
code in their project and release the final work under their own terms. See the
full [license](./LICENSE) text.

Notable users
-------------

- [cowsql](https://github.com/cowsql/cowsql)

Credits
-------

Of course the biggest thanks goes to Diego Ongaro :) (the original author of the
Raft dissertation).

A lot of ideas and inspiration was taken from other Raft implementations such
as:

- CoreOS' Go implementation for [etcd](https://github.com/etcd-io/etcd/tree/master/raft)
- Hashicorp's Go [raft](https://github.com/hashicorp/raft)
- Willem's [C implementation](https://github.com/willemt/raft)
- LogCabin's [C++ implementation](https://github.com/logcabin/logcabin)
