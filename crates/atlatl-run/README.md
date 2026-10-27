# atlatl-run

The runner behind `atlatl run`: opens the engine and connections, schedules replications, extracts, stages and commits them with their state, applies CDC to serving targets, and writes the run report, events and progress.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Execution](https://github.com/camconn/atlatl/blob/master/spec/03-execution.md).
