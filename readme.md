# mstore

Mstore is a simple wrapper around [badgerDB](https://github.com/dgraph-io/badger) for platform applications that require a quick persistent cache close to the consumer. It's intended to be used in services that will function as sidecars.