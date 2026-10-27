# atlatl-core

The shared foundation: loading and validating project configuration (`atlatl.yaml`, replications, API specs, environments), building the plan, templating, secrets and redaction, the error taxonomy and retries, and the run report and event types.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [Configuration](https://github.com/camconn/atlatl/blob/master/spec/02-configuration.md), [Execution](https://github.com/camconn/atlatl/blob/master/spec/03-execution.md).
