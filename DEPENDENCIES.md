# Dependency upgrade qualification

This continues PR #2 under task cleanup20261016-2010. Its prior head
55abee3abc28246e307e0da00c42b4a562018cc1 is integrated with master
650dd2f3e2f457733dd0469b11062609ba6fba45. The already merged PR #17
compression fix remains selected at compress v1.18.7.

## Compatibility and selected versions

- go-utils v1.12.9 -> v1.17.1; zap v1.12.2 -> its compatible
  v1.19.3-0.20211118020215-b17f220cebee revision; msgp v1.1.2 -> v1.1.8.
- Viper v1.6.3 -> v1.10.1 retains the actual go-utils Settings consumer's
  YAML values, case-insensitive keys, flag precedence, typed lists and durations.
- Select JWT /v4 v4.5.2, x/crypto v0.57.0, x/net v0.60.0,
  x/text v0.42.0, x/tools v0.49.0 and their required compatible graph.
- The retained remote/configuration graph selects Consul API v1.31.2,
  memberlist v0.6.0, SFTP v1.11.0, retryablehttp v0.7.7, Logrus v1.9.3,
  Prometheus client v1.23.2, protobuf v1.36.8 and Goldmark v1.7.17.
  These are graph maintenance changes; a dependency appearing in go.sum
  does not establish selection, import or production reachability.

The exported Logger remains *utils.LoggerType, including its embedded
*zap.Logger. WithLogger and NewLegacyLoader retain their original concrete
function signatures, including use as function values. The previous PR
changed these types; TestRegressionLoggerExportedConcreteType fails at the
assertion level before the compatibility correction. Compile-time contracts
and real journal start/write/Sync plus ACK membership tests cover the fix.

go-utils v1.17.1 factory functions return utils.LoggerItf. New callers can use
the additive WithLoggerInterface and NewLegacyLoaderWithLoggerInterface
entry points with factory results, Named/With results or custom implementations.
Code using the upgraded go-utils interface-returning helpers should choose
these additive entry points; the original concrete function types are retained. Internal storage
uses the interface without narrowing a custom logger. WithLogger and
WithLoggerInterface reject nil and typed-nil *utils.LoggerType values.
Other custom implementations must be non-nil usable logger values.

The Go floor remains 1.27.0. Intentional coreos/etcd legacy test imports and
the existing RoaringBitmap and codec versions are retained. There is no
WAL format, ACK ownership, reclamation, durability-barrier or TTL policy
change.

## Bounded security and false-positive controls

The dependency tests use bounded memory, loopback HTTP, or fake transports.
They check malformed SFTP length allocation, malformed protobuf JSON
termination, Consul request Content-Type, retry-log password redaction,
bounded unknown-method metric labels, long Logrus lines, and rejection of
an entity-encoded dangerous Markdown link while allowing HTTPS. S2 tests
reject overflowing repeat indices and accept a valid boundary dictionary;
they do not encode unsafe malformed input.

The JWT regression rejects a 256 KiB periods-only token with bounded
allocation, checks invalid-signature precedence over expiry, and accepts a
valid signed token. The selected identity is github.com/golang-jwt/jwt/v4
v4.5.2, not the unversioned JWT module or /v5. An old lower-bound graph edge
does not override Go's selected version.

Upstream evidence:
- [v4.5.1 error-handling fix](https://github.com/golang-jwt/jwt/releases/tag/v4.5.1)
- [v4.5.2 security release](https://github.com/golang-jwt/jwt/releases/tag/v4.5.2)

The historical Snyk report mapped a /v5 advisory to an unversioned JWT
v4.5.2 label and another unversioned advisory to the actual /v4 module.
The original PR records support reference 0D5PU00002SeEci0AF. That is
historical submitted evidence, not proof the service has corrected its
mapping today. No ignore, advisory override, fabricated passing status or
new external support message is part of this continuation. PR #2 remains
Draft until its current scanner/review gates are resolved.

## Validation boundary

The automatic fast gate now requires 12 named root tests and 10 dependency
contracts, including Linux flock/OFD lock interoperability. Its existing
eight gate tests reject missing, skipped, duplicate, failed or timed-out
execution evidence. Full ordinary/race and recovery commands retain
readonly modules and the 180-second package timeout. Five full race rounds
run in separate processes so the cumulative duration does not exhaust a
single package budget. The failed original in-process five-round attempt
is retained in task evidence; isolated rounds do not qualify repeated
execution in one process. Their actual results
are reported in the PR description and saved task evidence.

Existing WAL recovery, ACK/dedup/bitmap, plain/gzip, corruption, partial
append and subprocess tests remain active. Passing these tests does not
establish physical-power-loss qualification, application-level exactly-once
delivery, production capacity, or compatibility of every remote
configuration service deployment. Module, imported-package and called-symbol
scan results are reported separately.
