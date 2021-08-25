# Ramper

Trying to reinvent the wheel by writing a crawler. Main inspiration and target should be
[BUbiNG](https://github.com/LAW-Unimi/BUbiNG) and the corresponding
[paper](https://vigna.di.unimi.it/ftp/papers/BUbiNG.pdf)

### Dependencies

Currently the [dll](https://github.com/FiV0/dll.git) repository also needs to be accessible locally.

### Building

TODO: explain tools.build options

When developing you need to build the java files once before jacking in.

```bash
clojure -T:build java
```

The `pom.xml` at the root of the repo currently only serves for easier Java development
in combination with IDEs that support Maven integration.

### Testing

The tests can be run with
```bash
clojure -M:test -m cognitect.test-runner
```

If one wants to run a specific test, use the `-X` option. See also [cognitect.test-runner](https://github.com/cognitect-labs/test-runner) for options which tests to invoke.
```bash
clojure -X:test :nses ['ramper.workers.parsing-thread-test]
```

## License
