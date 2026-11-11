# Security policy

## Supported versions

kv0 is pre-1.0. Security fixes go into the latest release.

## Reporting a vulnerability

Please do not open a public issue. Report vulnerabilities privately through
[GitHub security advisories](https://github.com/sayef/kv0/security/advisories/new).

Include what you found, how to reproduce it and what you think the impact is. We will acknowledge the
report within five working days and keep you informed until it is resolved. We credit reporters in the
advisory unless you prefer otherwise.

## Scope

kv0 reads and writes Lance datasets at locations you configure, and trusts storage that only
authorised writers can modify. Credentials are passed to the object store as storage options or taken
from the environment; kv0 does not log or persist them. Reports of crashes, out-of-bounds reads or
excessive resource use from data kv0 itself wrote are in scope, as are credential leaks.
