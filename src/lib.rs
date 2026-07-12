// SPDX-License-Identifier: Apache-2.0
//! Apache Arrow IPC selected-wire payload support for Eclipse uProtocol.
//!
//! This crate encodes one uncompressed [`RecordBatch`] as one complete Arrow
//! IPC stream. Decoders require exactly one record batch and an exact terminal
//! IPC EOS marker, reject trailing bytes, and apply [`MAX_ARROW_PAYLOAD_LEN`]
//! before allocating for reader input.
//!
//! [`TelemetryTableV1`] demonstrates a deliberately narrow evolution policy:
//! columns may be reordered and unknown columns may be added, but all required
//! columns must remain present, non-null, and exact-type. This policy does not
//! imply automatic semantic-version tolerance for other payload mappings.

use std::io::{self, Read, Write};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_ipc::MessageHeader;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use bytes::Bytes;
use up_rust::selected_wire_user_api::{UNativePrefixWireTransport, UWithNativePrefixWire};
use up_rust::wire_implementer_api::{
    UProtocolNativeWire, UWire, UWirePayload, WireIdentity, NATIVE_PREFIX_METADATA_LAYOUT_ID,
};
use up_rust::{
    DecodePayload, EncodePayload, PayloadEncoding, PayloadFormat, PayloadLayout, ReadDecodePayload,
    UWireError,
};

/// Maximum accepted or produced Arrow IPC payload size (64 MiB).
pub const MAX_ARROW_PAYLOAD_LEN: usize = 64 * 1024 * 1024;

/// Provisional local/experimental selected-wire identity.
///
/// Compact ID `0xA201` is not a globally registered identity.
pub const ARROW_WIRE_ID: WireIdentity = WireIdentity::new(
    "org.eclipse.uprotocol.wire.arrow-ipc-stream.experimental",
    0xA201,
);

/// Provisional local/experimental payload-family identity.
///
/// Compact ID `0xA202` is not a globally registered identity.
pub const ARROW_PAYLOAD_FAMILY_ID: WireIdentity = WireIdentity::new(
    "org.eclipse.uprotocol.payload.arrow-ipc-stream.experimental",
    0xA202,
);

/// Payload encoding identifier carried in frame metadata.
pub const ARROW_ENCODING_ID: &str = "up.arrow-ipc-stream";

/// MIME media type for an Arrow IPC stream.
pub const ARROW_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

const IPC_CONTINUATION_MARKER: u32 = 0xFFFF_FFFF;
const IPC_EOS: [u8; 8] = [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];

/// Apache Arrow IPC selected-wire marker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArrowWire;

/// Native-prefix transport shape for [`ArrowWire`].
pub type ArrowNativePrefixTransport<TCore> = UNativePrefixWireTransport<TCore, ArrowWire>;

/// Wraps an encoded transport core with the Arrow native-prefix selected wire.
#[must_use]
pub fn with_arrow_native_prefix<TCore>(core: TCore) -> ArrowNativePrefixTransport<TCore> {
    core.into_native_prefix_wire_transport(ArrowWire)
}

impl UWire for ArrowWire {
    const WIRE_ID: WireIdentity = ARROW_WIRE_ID;
    const PAYLOAD_FAMILY_ID: WireIdentity = ARROW_PAYLOAD_FAMILY_ID;
    const METADATA_LAYOUT_ID: WireIdentity = NATIVE_PREFIX_METADATA_LAYOUT_ID;
    const FORMAT_VERSION: u16 = UProtocolNativeWire::FORMAT_VERSION;
}

impl PayloadFormat for ArrowWire {
    fn name() -> &'static str {
        "arrow-ipc-stream"
    }

    fn encoding() -> PayloadEncoding {
        PayloadEncoding::custom(ARROW_ENCODING_ID, ARROW_CONTENT_TYPE)
            .expect("static Arrow payload encoding is valid")
    }
}

/// Conversion contract between an application type and one Arrow record batch.
pub trait ArrowWirePayload: Sized {
    /// Converts this value to the wire's single record batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the value cannot be represented as a valid batch.
    fn to_record_batch(&self) -> Result<RecordBatch, UWireError>;

    /// Reconstructs a value from the wire's single record batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the batch does not satisfy this type's schema policy.
    fn from_record_batch(batch: RecordBatch) -> Result<Self, UWireError>;
}

impl<T> UWirePayload<T> for ArrowWire
where
    T: ArrowWirePayload,
{
    type Codec = Self;
}

impl<T> EncodePayload<T> for ArrowWire
where
    T: ArrowWirePayload,
{
    fn payload_layout(value: &T) -> Result<PayloadLayout, UWireError> {
        let bytes = encode_value(value)?;
        PayloadLayout::new(bytes.len(), 1)
    }

    fn encode_payload(value: &T, dst: &mut [u8]) -> Result<(), UWireError> {
        let bytes = encode_value(value)?;
        let actual = dst.len();
        let out = dst
            .get_mut(..bytes.len())
            .ok_or_else(|| UWireError::buffer_too_small(bytes.len(), actual))?;
        out.copy_from_slice(&bytes);
        Ok(())
    }

    fn encode_payload_owned(value: &T) -> Result<Bytes, UWireError> {
        encode_value(value).map(Bytes::from)
    }
}

impl<'a, T> DecodePayload<'a, T> for ArrowWire
where
    T: ArrowWirePayload,
{
    fn decode_payload(src: &'a [u8]) -> Result<T, UWireError> {
        decode_value(src)
    }
}

impl<T> ReadDecodePayload<T> for ArrowWire
where
    T: ArrowWirePayload,
{
    fn decode_payload_from_reader<R: Read>(
        mut reader: R,
        payload_len: usize,
    ) -> Result<T, UWireError> {
        ensure_payload_limit(payload_len)?;
        let mut bytes = vec![0_u8; payload_len];
        reader.read_exact(&mut bytes).map_err(|error| {
            UWireError::invalid_payload(format!(
                "Arrow payload reader did not yield the declared {payload_len} bytes: {error}"
            ))
        })?;
        decode_value(&bytes)
    }
}

fn encode_value<T: ArrowWirePayload>(value: &T) -> Result<Vec<u8>, UWireError> {
    let batch = value.to_record_batch()?;
    let mut output = BoundedWriter::new(MAX_ARROW_PAYLOAD_LEN);
    {
        let mut writer = StreamWriter::try_new(&mut output, batch.schema_ref())
            .map_err(serialization_error("create Arrow IPC stream"))?;
        writer
            .write(&batch)
            .map_err(serialization_error("write Arrow record batch"))?;
        writer
            .finish()
            .map_err(serialization_error("finish Arrow IPC stream"))?;
    }
    Ok(output.into_inner())
}

fn decode_value<T: ArrowWirePayload>(src: &[u8]) -> Result<T, UWireError> {
    ensure_payload_limit(src.len())?;
    validate_ipc_envelope(src)?;

    let mut reader =
        StreamReader::try_new(src, None).map_err(invalid_payload_error("open Arrow IPC stream"))?;
    let batch = reader
        .next()
        .ok_or_else(|| UWireError::invalid_payload("Arrow IPC stream contains no record batch"))?
        .map_err(invalid_payload_error("decode Arrow record batch"))?;
    if reader.next().is_some() {
        return Err(UWireError::invalid_payload(
            "Arrow IPC stream contains more than one record batch",
        ));
    }
    T::from_record_batch(batch)
}

fn validate_ipc_envelope(src: &[u8]) -> Result<(), UWireError> {
    let mut offset = 0_usize;
    let mut batches = 0_usize;

    loop {
        let first = read_u32(src, offset, "Arrow IPC message prefix")?;
        offset = checked_add(offset, 4, "Arrow IPC prefix offset")?;

        let metadata_len = if first == IPC_CONTINUATION_MARKER {
            let len = read_u32(src, offset, "Arrow IPC continuation length")?;
            offset = checked_add(offset, 4, "Arrow IPC continuation offset")?;
            if len == 0 {
                if src.get(offset - IPC_EOS.len()..offset) != Some(IPC_EOS.as_slice()) {
                    return Err(UWireError::invalid_payload(
                        "Arrow IPC stream has a non-canonical EOS marker",
                    ));
                }
                if offset != src.len() {
                    return Err(UWireError::invalid_payload(
                        "Arrow IPC stream has trailing bytes after EOS",
                    ));
                }
                break;
            }
            usize::try_from(len)
                .map_err(|_| UWireError::invalid_payload("Arrow IPC metadata length overflow"))?
        } else {
            if first == 0 {
                return Err(UWireError::invalid_payload(
                    "Arrow IPC stream must end with the eight-byte continuation/EOS marker",
                ));
            }
            usize::try_from(first)
                .map_err(|_| UWireError::invalid_payload("Arrow IPC metadata length overflow"))?
        };

        let metadata_end = checked_add(offset, metadata_len, "Arrow IPC metadata length")?;
        let metadata = src.get(offset..metadata_end).ok_or_else(|| {
            UWireError::invalid_payload("Arrow IPC stream has truncated message metadata")
        })?;
        let message = arrow_ipc::root_as_message(metadata)
            .map_err(invalid_payload_error("parse Arrow IPC message metadata"))?;

        if message.header_type() == MessageHeader::RecordBatch {
            let record_batch = message.header_as_record_batch().ok_or_else(|| {
                UWireError::invalid_payload("Arrow IPC record-batch metadata is missing")
            })?;
            if record_batch.compression().is_some() {
                return Err(UWireError::invalid_payload(
                    "compressed Arrow IPC record batches are not supported",
                ));
            }
            batches = batches
                .checked_add(1)
                .ok_or_else(|| UWireError::invalid_payload("Arrow IPC batch count overflow"))?;
        }

        let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
            UWireError::invalid_payload("Arrow IPC message has a negative or oversized body length")
        })?;
        offset = checked_add(metadata_end, body_len, "Arrow IPC body length")?;
        if offset > src.len() {
            return Err(UWireError::invalid_payload(
                "Arrow IPC stream has a truncated message body",
            ));
        }
    }

    match batches {
        1 => Ok(()),
        0 => Err(UWireError::invalid_payload(
            "Arrow IPC stream contains no record batch",
        )),
        _ => Err(UWireError::invalid_payload(
            "Arrow IPC stream contains more than one record batch",
        )),
    }
}

fn ensure_payload_limit(len: usize) -> Result<(), UWireError> {
    if len > MAX_ARROW_PAYLOAD_LEN {
        return Err(UWireError::invalid_payload(format!(
            "Arrow IPC payload length {len} exceeds the {MAX_ARROW_PAYLOAD_LEN}-byte limit"
        )));
    }
    Ok(())
}

fn read_u32(src: &[u8], offset: usize, context: &'static str) -> Result<u32, UWireError> {
    let end = checked_add(offset, 4, context)?;
    let bytes: [u8; 4] = src
        .get(offset..end)
        .ok_or_else(|| UWireError::invalid_payload(format!("{context} is truncated")))?
        .try_into()
        .map_err(|_| UWireError::invalid_payload(format!("{context} is malformed")))?;
    Ok(u32::from_le_bytes(bytes))
}

fn checked_add(left: usize, right: usize, context: &'static str) -> Result<usize, UWireError> {
    left.checked_add(right)
        .ok_or_else(|| UWireError::invalid_payload(format!("{context} overflow")))
}

fn serialization_error(context: &'static str) -> impl FnOnce(ArrowError) -> UWireError {
    move |error| UWireError::serialization_error(format!("{context}: {error}"))
}

fn invalid_payload_error<E>(context: &'static str) -> impl FnOnce(E) -> UWireError
where
    E: std::fmt::Display,
{
    move |error| UWireError::invalid_payload(format!("{context}: {error}"))
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(1024),
            limit,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let new_len = self
            .bytes
            .len()
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::other("Arrow IPC payload length overflow"))?;
        if new_len > self.limit {
            return Err(io::Error::other(format!(
                "Arrow IPC payload exceeds the {}-byte limit",
                self.limit
            )));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Reference telemetry payload used by conformance tests and benchmarks.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryTableV1 {
    /// Monotonic sample timestamps in nanoseconds.
    pub timestamps_ns: Vec<u64>,
    /// Sensor channel IDs, parallel to `timestamps_ns`.
    pub channel: Vec<u32>,
    /// Measured values, parallel to `timestamps_ns`.
    pub value: Vec<f64>,
}

impl TelemetryTableV1 {
    /// Builds a deterministic fixture with wrapping integer arithmetic.
    #[must_use]
    pub fn fixture(rows: usize, seed: u64) -> Self {
        let mut timestamps_ns = Vec::with_capacity(rows);
        let mut channel = Vec::with_capacity(rows);
        let mut value = Vec::with_capacity(rows);
        for index in 0..rows {
            let sample = index as u64;
            timestamps_ns.push(
                seed.wrapping_mul(1_000_000)
                    .wrapping_add(sample.wrapping_mul(500)),
            );
            channel.push((index % 16) as u32);
            value.push(((seed ^ sample) % 1_000) as f64 * 0.5);
        }
        Self {
            timestamps_ns,
            channel,
            value,
        }
    }
}

impl ArrowWirePayload for TelemetryTableV1 {
    fn to_record_batch(&self) -> Result<RecordBatch, UWireError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamps_ns", DataType::UInt64, false),
            Field::new("channel", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(self.timestamps_ns.clone())),
                Arc::new(UInt32Array::from(self.channel.clone())),
                Arc::new(Float64Array::from(self.value.clone())),
            ],
        )
        .map_err(serialization_error("build telemetry record batch"))
    }

    fn from_record_batch(batch: RecordBatch) -> Result<Self, UWireError> {
        let timestamps_ns =
            required_column::<UInt64Array>(&batch, "timestamps_ns", &DataType::UInt64)?;
        let channel = required_column::<UInt32Array>(&batch, "channel", &DataType::UInt32)?;
        let value = required_column::<Float64Array>(&batch, "value", &DataType::Float64)?;
        Ok(Self {
            timestamps_ns: timestamps_ns.values().to_vec(),
            channel: channel.values().to_vec(),
            value: value.values().to_vec(),
        })
    }
}

fn required_column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
    expected_type: &DataType,
) -> Result<&'a T, UWireError> {
    let index = batch.schema().index_of(name).map_err(|_| {
        UWireError::invalid_payload(format!("telemetry required column `{name}` is missing"))
    })?;
    let schema = batch.schema();
    let field = schema.field(index);
    if field.data_type() != expected_type {
        return Err(UWireError::invalid_payload(format!(
            "telemetry column `{name}` has type {}, expected {expected_type}",
            field.data_type()
        )));
    }
    if field.is_nullable() {
        return Err(UWireError::invalid_payload(format!(
            "telemetry required column `{name}` is declared nullable"
        )));
    }
    let column = batch.column(index);
    if column.null_count() != 0 {
        return Err(UWireError::invalid_payload(format!(
            "telemetry required column `{name}` contains null values"
        )));
    }
    column.as_any().downcast_ref::<T>().ok_or_else(|| {
        UWireError::invalid_payload(format!(
            "telemetry column `{name}` cannot be downcast to its declared type"
        ))
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Error, ErrorKind};

    use arrow_array::{ArrayRef, Int64Array, StringArray};

    use super::*;

    fn encode(table: &TelemetryTableV1) -> Vec<u8> {
        <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload_owned(table)
            .expect("encode fixture")
            .to_vec()
    }

    fn encode_batches(batches: &[RecordBatch], schema: &Arc<Schema>) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, schema).expect("create stream");
            for batch in batches {
                writer.write(batch).expect("write batch");
            }
            writer.finish().expect("finish stream");
        }
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<TelemetryTableV1, UWireError> {
        <ArrowWire as DecodePayload<'_, TelemetryTableV1>>::decode_payload(bytes)
    }

    fn assert_invalid(result: Result<TelemetryTableV1, UWireError>) {
        assert!(matches!(result, Err(UWireError::InvalidPayload(_))));
    }

    #[test]
    fn round_trip_contiguous_and_reader() {
        let expected = TelemetryTableV1::fixture(4_096, 7);
        let bytes = encode(&expected);
        assert_eq!(decode(&bytes).expect("contiguous decode"), expected);
        let actual =
            <ArrowWire as ReadDecodePayload<TelemetryTableV1>>::decode_payload_from_reader(
                Cursor::new(&bytes),
                bytes.len(),
            )
            .expect("reader decode");
        assert_eq!(actual, expected);
    }

    #[test]
    fn layout_matches_owned_encoding() {
        let table = TelemetryTableV1::fixture(128, 3);
        let layout =
            <ArrowWire as EncodePayload<TelemetryTableV1>>::payload_layout(&table).expect("layout");
        assert_eq!(layout.align(), 1);
        assert_eq!(layout.len(), encode(&table).len());
    }

    #[test]
    fn encoded_stream_has_exact_terminal_eos() {
        let bytes = encode(&TelemetryTableV1::fixture(4, 1));
        assert_eq!(
            bytes.get(bytes.len() - IPC_EOS.len()..),
            Some(IPC_EOS.as_slice())
        );
        assert_eq!(
            bytes
                .windows(IPC_EOS.len())
                .filter(|window| *window == IPC_EOS)
                .count(),
            1
        );
    }

    #[test]
    fn reordered_and_additive_columns_are_accepted() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, false),
            Field::new("extra", DataType::Utf8, false),
            Field::new("timestamps_ns", DataType::UInt64, false),
            Field::new("channel", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![1.5, 2.5])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![3, 4])) as ArrayRef,
            ],
        )
        .expect("batch");
        let bytes = encode_batches(&[batch], &schema);
        assert_eq!(
            decode(&bytes).expect("decode"),
            TelemetryTableV1 {
                timestamps_ns: vec![10, 20],
                channel: vec![3, 4],
                value: vec![1.5, 2.5],
            }
        );
    }

    #[test]
    fn wrong_required_type_is_rejected() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamps_ns", DataType::Int64, false),
            Field::new("channel", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![2])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3.0])) as ArrayRef,
            ],
        )
        .expect("batch");
        assert_invalid(decode(&encode_batches(&[batch], &schema)));
    }

    #[test]
    fn nullable_required_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamps_ns", DataType::UInt64, true),
            Field::new("channel", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![2, 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3.0, 4.0])) as ArrayRef,
            ],
        )
        .expect("batch");
        assert_invalid(decode(&encode_batches(&[batch], &schema)));
    }

    #[test]
    fn null_required_value_is_rejected() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamps_ns", DataType::UInt64, true),
            Field::new("channel", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![2, 3])) as ArrayRef,
                Arc::new(Float64Array::from(vec![3.0, 4.0])) as ArrayRef,
            ],
        )
        .expect("nullable batch");
        assert_invalid(decode(&encode_batches(&[batch], &schema)));
    }

    #[test]
    fn missing_required_column_is_rejected() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamps_ns", DataType::UInt64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(UInt64Array::from(vec![1])) as ArrayRef,
                Arc::new(Float64Array::from(vec![2.0])) as ArrayRef,
            ],
        )
        .expect("batch");
        assert_invalid(decode(&encode_batches(&[batch], &schema)));
    }

    #[test]
    fn no_batch_is_rejected() {
        let schema = TelemetryTableV1::fixture(1, 1)
            .to_record_batch()
            .expect("batch")
            .schema();
        assert_invalid(decode(&encode_batches(&[], &schema)));
    }

    #[test]
    fn multiple_batches_are_rejected() {
        let batch = TelemetryTableV1::fixture(1, 1)
            .to_record_batch()
            .expect("batch");
        let bytes = encode_batches(&[batch.clone(), batch.clone()], &batch.schema());
        assert_invalid(decode(&bytes));
    }

    #[test]
    fn missing_eos_is_rejected() {
        let mut bytes = encode(&TelemetryTableV1::fixture(1, 1));
        bytes.truncate(bytes.len() - IPC_EOS.len());
        assert_invalid(decode(&bytes));
    }

    #[test]
    fn truncated_eos_is_rejected() {
        let mut bytes = encode(&TelemetryTableV1::fixture(1, 1));
        bytes.truncate(bytes.len() - 1);
        assert_invalid(decode(&bytes));
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = encode(&TelemetryTableV1::fixture(1, 1));
        bytes.push(0);
        assert_invalid(decode(&bytes));
    }

    #[test]
    fn second_eos_is_rejected_as_trailing_data() {
        let mut bytes = encode(&TelemetryTableV1::fixture(1, 1));
        bytes.extend_from_slice(&IPC_EOS);
        assert_invalid(decode(&bytes));
    }

    #[test]
    fn malformed_stream_is_rejected() {
        assert_invalid(decode(b"not an Arrow IPC stream"));
    }

    struct ShortReader;

    impl Read for ShortReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(Error::new(ErrorKind::UnexpectedEof, "short fixture"))
        }
    }

    #[test]
    fn short_reader_is_rejected() {
        assert_invalid(
            <ArrowWire as ReadDecodePayload<TelemetryTableV1>>::decode_payload_from_reader(
                ShortReader,
                16,
            ),
        );
    }

    struct PanicReader;

    impl Read for PanicReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            panic!("oversized input must be rejected before reading")
        }
    }

    #[test]
    fn oversized_declared_reader_input_is_rejected_before_allocation() {
        assert_invalid(
            <ArrowWire as ReadDecodePayload<TelemetryTableV1>>::decode_payload_from_reader(
                PanicReader,
                MAX_ARROW_PAYLOAD_LEN + 1,
            ),
        );
    }

    #[test]
    fn destination_size_uses_prefix_write_semantics() {
        let table = TelemetryTableV1::fixture(8, 4);
        let expected = encode(&table);
        let mut short = vec![0_u8; expected.len() - 1];
        assert_eq!(
            <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload(&table, &mut short),
            Err(UWireError::BufferTooSmall {
                expected: expected.len(),
                actual: expected.len() - 1,
            })
        );

        let mut large = vec![0xA5; expected.len() + 7];
        <ArrowWire as EncodePayload<TelemetryTableV1>>::encode_payload(&table, &mut large)
            .expect("encode into larger destination");
        assert_eq!(&large[..expected.len()], expected.as_slice());
        assert!(large[expected.len()..].iter().all(|byte| *byte == 0xA5));
    }

    #[test]
    fn identities_are_distinct_provisional_experimental_values() {
        assert_ne!(ARROW_WIRE_ID, ARROW_PAYLOAD_FAMILY_ID);
        for identity in [ARROW_WIRE_ID, ARROW_PAYLOAD_FAMILY_ID] {
            assert!((0x8000..=0xFFFE).contains(&identity.compact_id()));
            assert!(identity.literal_id().contains("experimental"));
        }
        assert_eq!(ArrowWire::WIRE_ID, ARROW_WIRE_ID);
        assert_eq!(ArrowWire::PAYLOAD_FAMILY_ID, ARROW_PAYLOAD_FAMILY_ID);
        assert_eq!(
            ArrowWire::METADATA_LAYOUT_ID,
            NATIVE_PREFIX_METADATA_LAYOUT_ID
        );
        assert_eq!(
            ArrowWire::FORMAT_VERSION,
            UProtocolNativeWire::FORMAT_VERSION
        );
    }
}
