# up-wire-arrow

`up-wire-arrow` is a non-published Rust 1.88 crate implementing an Apache Arrow
IPC selected-wire payload codec for Eclipse uProtocol. It composes with an
up-rust encoded transport core through the canonical native-prefix metadata
adapter; it is not a physical transport.

## Payload Contract

The payload is one complete, uncompressed Arrow IPC stream containing exactly
one `RecordBatch`. The terminal eight-byte continuation/EOS marker must end the
payload. Decoding rejects absent or additional batches, missing or truncated
EOS, trailing bytes, malformed streams, short or overlong readers, and payloads
larger than [`MAX_ARROW_PAYLOAD_LEN`](src/lib.rs). The limit is checked before a
reader allocation, and encoding uses a bounded writer.

`TelemetryTableV1` is the reference mapping. It identifies required columns by
name, so reordering and additive columns are accepted. Its three required
columns must be non-null and exactly `UInt64`, `UInt32`, and `Float64`. This is a
specific schema policy, not a claim that arbitrary Arrow schemas or semantic
versions are automatically compatible.

## Identities

The compact IDs `0xA201` (wire) and `0xA202` (payload family) are unique within
this crate and lie in up-rust's `0x8000..=0xFFFE` local/experimental range. They
are provisional and are not globally registered interoperability identities.

Selecting `ArrowWire` also opts into deployment-private payload encoding `0xF002`
for the exact IPC stream contract above. Peers must agree to and reserve that
assignment across their shared private-ID budget, including native-profile tables.
It is not a public registry allocation; the earlier proposed public ID 10 is
unassigned and is no longer emitted. This candidate reserves `0xF001`/`0xF002`/
`0xF003` for XCDRv2/Arrow/OMG IDL serialized profiles. Native structural tokens are
separate metadata and are not produced by the Arrow codec.

## Encoding Costs

Arrow IPC has dynamic size. `payload_layout` performs one complete probe
serialization. `encode_payload` performs another serialization into a temporary
bounded buffer before copying its encoded prefix to the destination. The pinned
up-rust trait permits an `encode_payload_owned` override, which this crate uses
to serialize exactly once. A loaned flow that probes and then directly encodes
therefore serializes twice; owned encode serializes once.

## Validation

```text
cargo +1.88.0 check --locked --all-targets
cargo +1.95.0 fmt --check
cargo +1.95.0 clippy --locked --all-targets -- -D warnings
cargo +1.95.0 test --locked --all-targets
RUSTDOCFLAGS="-D warnings" cargo +1.95.0 doc --locked --no-deps
cargo +1.95.0 test --locked --doc
cargo +1.95.0 check --locked --benches
cargo +1.95.0 package --list
cargo +1.95.0 tree --locked -e features
cargo deny check advisories licenses bans sources
```

The Criterion benchmark keeps owned encode, layout probe, direct encode,
contiguous decode, and reader decode in separate groups for 64, 4,096, and
65,536 rows.

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
