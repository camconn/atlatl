# atlatl-http

The declarative REST API engine behind `type: rest` connections: request templating, authentication (API keys, OAuth2, HMAC signing, …), pagination, `for_each` iteration and queues, response rules, a shared rate limiter, record extraction and sync state.

Part of [atlatl](https://github.com/camconn/atlatl): an internal piece of the `atlatl` command line ([`atlatl-cli`](https://crates.io/crates/atlatl-cli)). It's published so the CLI can be, and its API isn't stable: it can change in any release.

Spec: [REST API engine](https://github.com/camconn/atlatl/blob/master/spec/07-rest-api.md).
