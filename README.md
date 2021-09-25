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
clojure -X:test :nses [ramper.workers.parsing-thread-test]
```

Running the crawler against a local graph server we can use [BUbiNG](https://github.com/LAW-Unimi/BUbiNG). The following will start a server on port 8080 with a 100 Million sites,
average page degree 50, average depth 3 and 0.01% of sites being broken.
```bash
java -cp bubing-0.9.15.jar:bubing-0.9.15-deps/* -Xmx4G -server it.unimi.di.law.bubing.test.NamedGraphServerHttpProxy -s 100000000 -d 50 -m 3 -t 1000 -D .0001 -A1000 -
```
The precompiled jars can be found at [http://law.di.unimi.it/software/index.php?path=download/](http://law.di.unimi.it/software/index.php?path=download/).

### Profiling

For Java mission control to work correctly you need to set

```bash
echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid
```

## License
