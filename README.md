# market-data-link

Shared Rust wire-contract definitions for market-data links.

This crate intentionally contains only message schemas and codecs used between
MDP, FeaturesModule, and Trading Engine. It does not own transport mechanics,
connection lifecycle, subscription policy, routing, exchange state, feature
calculation, or trading-engine behavior.

## Modules

- `client_messages`: JSON control messages sent by market-data clients.
- `server_messages`: JSON control replies and asynchronous stream errors sent by servers.
- `mdp_messages`: binary market-data payloads produced by MDP.
- `feature_messages`: binary feature payloads produced by FeaturesModule.

Services should depend on this crate via a local path in the shared checkout and
keep all service-specific behavior in their own repos.
