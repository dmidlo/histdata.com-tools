# Logging and secret handling

This example intentionally has no logger or credentials. Generated event JSON
can be printed only in the explicit demonstration. Production implementations
must not emit provider responses, request URLs/query strings, configuration,
headers, authorization tokens, account identifiers or raw exception/traceback
text to stdout/stderr: supervised hosts bound and discard those streams.

Author diagnostics from `BrokerReasonCode` and fixed public summaries. Keep
retryability advisory; it never authorizes a retry. Catch provider exceptions and
translate them to `BrokerPluginError` without interpolation. Redaction regexes
are hygiene, not proof that arbitrary text is secret-free. Omit unreviewed
extensions rather than hiding credentials under innocuous field names.

Real integrations must use operator-reviewed opaque resource profiles. With
`resource_abi="host_resources_v1"`, a factory accepts the public
`BrokerHostResourcesV1` protocol. Its `request(endpoint_id, method, path,
secret_profile=opaque_profile_id)` allows the host to inject credentials only
into approved transport. It never exports the credential to the plugin. Exact
endpoints, path/method/byte/deadline bounds and secret profile names belong in the
permission manifest; do not grant arbitrary network access or silently fall
back to direct sockets. The current subprocess resource operation is unsupported.

No real credentials belong in this repository, conformance data, CI secrets,
configuration fixtures, wheel metadata, artifact IDs, source hashes, or logs.
The intentionally malformed fixture modes exist solely to exercise rejection;
never enable them for a real provider. Close resources in `finally`, even on
cancellation or authorization refusal, without pulling more provider messages.

