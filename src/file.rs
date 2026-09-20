//! File upload/download helpers.
//!
//! The types this module defines are re-exported at the crate root — the two
//! option types ([`crate::FileUploadOptions`],
//! [`crate::FileStreamUploadOptions`]) and the two
//! [`RociaDbClient::stat_file`] returns ([`crate::FileMetadata`],
//! [`crate::FileTimestamp`]) — and the RPCs are inherent methods on
//! [`RociaDbClient`]. [`RociaDbClient::upload_file_stream`] documents the
//! server's upload wire contract in full — including the 1 MiB per-message cap
//! every upload path here respects.
use crate::error::StatusResultExt;
use crate::pb::upstream::v1::{
    DeleteRequest, DownloadRequest, DownloadResponse, ListBucketsRequest, ListFilesRequest,
    StatRequest, StatResponse, UploadRequest,
};
use crate::{
    Bytes, DEFAULT_PAGE_SIZE, Page, Result, RociaDbClient, RociaDbError, WriteOptions, non_empty,
    page_request,
};
use futures::{Stream, StreamExt, stream};
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tonic::codec::Streaming;
use tracing::{debug, warn};
use uuid::Uuid;

/// Size of every upload message the SDK emits, except the last one. Not
/// configurable: see [`RociaDbClient::upload_file_stream`].
const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB.

/// `operation` tag on every error the three upload paths produce, and the label
/// the pre-flight refresh and the replay log under. One constant because all
/// three go through [`RociaDbClient::upload_raw`], which must name the same
/// operation as the helpers built on it.
const UPLOAD_OPERATION: &str = "failed to upload file";

/// `operation` tag for the call that opens a download stream. Not shared with
/// the error [`RociaDbClient::download_file`] reports for a failure *during* the
/// stream (`"file download stream failed"`), which is a different event: the
/// call had already been accepted.
const DOWNLOAD_OPERATION: &str = "failed to start file download";

/// Client-side max file size applied when
/// [`RociaDbBuilder::max_file_bytes`](crate::RociaDbBuilder::max_file_bytes) was
/// never called: 5 GiB, which is the server's own `limits.max_file_bytes`
/// default.
///
/// Only a default, and only a mirror. The server's limit is configurable, so a
/// deployment that raised or lowered it moves the number that actually decides
/// — the client-side gate exists to fail an obviously oversized upload before a
/// byte goes out, never to be the authority. See that setter.
pub(crate) const DEFAULT_MAX_FILE_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// `context` on the [`RociaDbError::Io`] that
/// [`RociaDbClient::download_file_verified_to`] reports when the caller's writer
/// refuses bytes. One constant because the per-chunk write and the final flush
/// are the same failure as far as a caller is concerned: the writer would not
/// take the download.
const DOWNLOAD_WRITE_CONTEXT: &str = "writing the downloaded file";

/// Ceiling on the buffer [`RociaDbClient::download_file_verified`]
/// pre-allocates from the `size_bytes` the server reported.
///
/// The size is the server's word, not a measurement, so allocating it blindly
/// would let a compromised or simply buggy server make the client reserve
/// gigabytes before a single byte of the file has arrived. 64 MiB is large
/// enough that every realistic file is allocated exactly once, and small
/// enough to be an unremarkable allocation if the number is nonsense; a file
/// genuinely larger than this just grows its buffer while streaming, the same
/// way [`RociaDbClient::download_file`] always does.
///
/// There is nothing to cap in [`RociaDbClient::download_file_verified_to`],
/// which pre-allocates nothing at all: it holds one chunk at a time and writes
/// it straight out.
pub(crate) const MAX_PREALLOCATED_DOWNLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// Default MIME type recorded for a file whose uploader did not name one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// `context` on the [`RociaDbError::Decode`] that [`FileTimestamp::system_time`]
/// and [`FileTimestamp::unix_nanos`] report for a string they cannot parse. One
/// constant because both run the same parser, so a caller matching on the
/// context must see one value from either.
const FILE_TIMESTAMP_CONTEXT: &str = "file timestamp";

/// How much of an unparseable timestamp [`FileTimestamp::decode_error`] quotes.
///
/// The value is the server's, so its length is not this crate's to trust: a
/// server answering `Stat` with a megabyte in `created_at` must not turn every
/// log line about it into a megabyte. Comfortably longer than any timestamp a
/// server plausibly sends — a date, a time, nine fractional digits and a
/// `±hh:mm` offset is 35 characters — so a realistic mistake is quoted whole.
const MAX_QUOTED_TIMESTAMP_CHARS: usize = 64;

/// Metadata recorded for one stored file, as returned by
/// [`RociaDbClient::stat_file`].
///
/// Describes the **published** version of the file: an upload still in flight
/// is invisible here, exactly as it is to `list_files` and the downloads (see
/// [`RociaDbClient::upload_file_stream`]).
///
/// This is an SDK-owned type rather than the protobuf message the server
/// answers with, which is what lets [`created_at`](Self::created_at) and
/// [`updated_at`](Self::updated_at) be [`FileTimestamp`]s — a value that keeps
/// the server's own text and parses it only when asked — instead of bare
/// [`String`]s. The other three fields carry exactly what the wire carries.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    /// Total size of the stored file in bytes.
    ///
    /// The server can be trusted with this one: it rejects an upload whose
    /// chunks do not add up to the `size_bytes` declared on the first message,
    /// so the number describes bytes it actually received. It is still not a
    /// measurement of what is stored *now*, which is why
    /// [`RociaDbClient::download_file_verified`] checks the download against
    /// it rather than assuming.
    pub size_bytes: u64,
    /// MIME type recorded at upload time, exactly as the uploader declared it.
    /// The server does not inspect the bytes to confirm it. The two ergonomic
    /// uploads always declare one — `"application/octet-stream"` when the caller
    /// named nothing (see [`FileUploadOptions::content_type`]) — so an empty
    /// value here means the file was written by
    /// [`RociaDbClient::upload_file_stream`], which declares nothing for you, or
    /// by another client altogether.
    pub content_type: String,
    /// SHA-256 digest recorded at upload time, as 32 raw bytes.
    ///
    /// **Never verified by the server.** It checks that the value the uploader
    /// sent is 32 bytes long and stores it; it never hashes the bytes it
    /// received to confirm the two agree. So this is what was *claimed* for the
    /// file, not a fact about its contents — which is the whole reason
    /// [`RociaDbClient::download_file_verified`] exists, and the limit of what
    /// that method can prove.
    ///
    /// Held as a `Vec<u8>` rather than a `[u8; 32]` because the length is the
    /// server's to report: the upload path rejects any other length, but
    /// nothing promises one on read, and a stored value that is not a 32-byte
    /// digest simply cannot match anything a verified download computes.
    pub checksum: Vec<u8>,
    /// When this `file_id` was **first** uploaded, as the server formatted it.
    ///
    /// Unchanged by a replacement: re-uploading an existing `file_id` moves
    /// [`updated_at`](Self::updated_at) and leaves this alone.
    pub created_at: FileTimestamp,
    /// When this `file_id` was **most recently** uploaded, as the server
    /// formatted it. Equal to [`created_at`](Self::created_at) until the file
    /// is replaced.
    pub updated_at: FileTimestamp,
}

impl From<StatResponse> for FileMetadata {
    fn from(response: StatResponse) -> Self {
        Self {
            size_bytes: response.size_bytes,
            content_type: response.content_type,
            checksum: response.checksum,
            created_at: FileTimestamp::new(response.created_at),
            updated_at: FileTimestamp::new(response.updated_at),
        }
    }
}

/// A timestamp on [`FileMetadata`], kept as the server wrote it and parsed
/// only on demand.
///
/// The protobuf field behind [`FileMetadata::created_at`] and
/// [`FileMetadata::updated_at`] is a plain `string`, and **nothing in the
/// schema says which format the server writes it in**. So this type does not
/// bet a `stat_file` call on a guess: it holds the server's own text, which
/// [`as_str`](Self::as_str) and [`Display`](std::fmt::Display) hand back
/// verbatim, and parses it only when a caller asks for an instant through
/// [`system_time`](Self::system_time) or [`unix_nanos`](Self::unix_nanos). A
/// server whose format this SDK cannot read therefore costs you those two
/// methods, and nothing else.
///
/// # The format this parses
///
/// RFC 3339 — the conventional wire form for an instant carried as a string,
/// and the profile of ISO 8601 that a date and time with an offset already
/// looks like:
///
/// ```text
/// 2026-09-19T14:03:07Z
/// 2026-09-19T14:03:07.250Z
/// 2026-09-19 14:03:07.123456789+02:00
/// 2026-09-19t14:03:07-05:30
/// ```
///
/// Precisely: a four-digit year, month and day separated by `-`; a `T` (either
/// case) or a single space; two-digit hours, minutes and seconds separated by
/// `:`; optionally a `.` and **any** number of fractional digits, of which the
/// first nine are kept and the rest truncated rather than rounded; and then
/// either `Z` (either case) or a `±hh:mm` offset. Every field is range-checked
/// against the calendar — `2000-02-29` is a date, `1900-02-29` and
/// `2100-02-29` are not — and **anything else is rejected**, including a
/// missing offset, an unpadded field, a `±hhmm` offset without its colon, and
/// trailing text of any kind.
///
/// Two deliberate decisions, both of which a caller can work around by reading
/// [`as_str`](Self::as_str) and parsing it themselves:
///
/// - **A leap second is rejected.** RFC 3339 allows `:60` for a positive leap
///   second; Unix time has no distinct instant to map it to, so
///   [`system_time`](Self::system_time) refuses it rather than silently moving
///   it by a second.
/// - **An instant before 1970 is supported**, as `UNIX_EPOCH - Duration`:
///   [`system_time`](Self::system_time) handles it, and
///   [`unix_nanos`](Self::unix_nanos) simply reports a negative number.
///
/// # Equality
///
/// [`PartialEq`] compares the raw strings, so two spellings of the same
/// instant (`2026-09-19T00:00:00Z` and `2026-09-19T02:00:00+02:00`) are **not**
/// equal. Compare [`system_time`](Self::system_time) or
/// [`unix_nanos`](Self::unix_nanos) when the instant is what matters.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTimestamp {
    /// The string the server sent, stored and never rewritten. Private because
    /// [`FileTimestamp::as_str`] is how it is read — one accessor rather than
    /// two ways to reach the same bytes.
    raw: String,
}

impl FileTimestamp {
    /// Wrap one timestamp string exactly as the server sent it.
    ///
    /// Crate-private on purpose: a `FileTimestamp` means "what the server
    /// reported for this file", and nothing validates the string here — the
    /// parse happens in [`FileTimestamp::system_time`], on demand.
    pub(crate) fn new(raw: impl Into<String>) -> Self {
        Self { raw: raw.into() }
    }

    /// The timestamp exactly as the server wrote it, parsed or not.
    ///
    /// Always available, whatever format the server used, and the fallback
    /// whenever [`system_time`](Self::system_time) reports
    /// [`RociaDbError::Decode`].
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// The instant this timestamp names, as a [`SystemTime`].
    ///
    /// Parses [the format above](Self#the-format-this-parses) on every call —
    /// nothing is cached, and the cost is a walk over a couple of dozen bytes.
    /// An instant before 1970 comes back as `UNIX_EPOCH - Duration`, so a
    /// caller who then wants a number should ask [`unix_nanos`](Self::unix_nanos)
    /// rather than `duration_since(UNIX_EPOCH)`, which fails for exactly those.
    ///
    /// Both [`chrono::DateTime<Utc>`](https://docs.rs/chrono) and
    /// [`time::OffsetDateTime`](https://docs.rs/time) implement
    /// `From<SystemTime>`, so this is the one hop to whichever date-time type
    /// a caller already uses:
    ///
    /// ```rust,no_run
    /// # use rociadb_sdk::RociaDbBuilder;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = RociaDbBuilder::new().disable_auth().build().await?;
    /// let metadata = client.stat_file("tenant-1", "assets", "manual.txt").await?;
    /// match metadata.updated_at.system_time() {
    ///     Ok(updated) => {
    ///         let age = std::time::SystemTime::now().duration_since(updated)?;
    ///         println!("last written {} seconds ago", age.as_secs());
    ///     }
    ///     // The server formatted it some other way; the text is still there.
    ///     Err(error) => println!("{}: {error}", metadata.updated_at),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`RociaDbError::Decode`] with `context` `"file timestamp"` when the
    /// string is not the format above — including when it is a perfectly good
    /// timestamp in another one — and when the instant is outside the range
    /// this platform's [`SystemTime`] can represent. The error quotes the
    /// offending value (bounded in length) and says what is wrong with it; its
    /// `source` is a [`serde_json::Error`] because that is the shape this
    /// variant has, not because any JSON was involved.
    pub fn system_time(&self) -> Result<SystemTime> {
        let (seconds, nanos) = self.unix_instant()?;
        unix_instant_to_system_time(seconds, nanos).ok_or_else(|| {
            self.decode_error("the instant is outside the range SystemTime can represent here")
        })
    }

    /// The instant this timestamp names, in nanoseconds since the Unix epoch —
    /// negative before it.
    ///
    /// The numeric counterpart of [`system_time`](Self::system_time), parsing
    /// exactly the same format and failing on exactly the same terms. Prefer it
    /// for ordering, differences and storage: it is one `i128` comparison, it
    /// needs no case for an instant before 1970 (where
    /// `SystemTime::duration_since(UNIX_EPOCH)` returns an error), and it
    /// cannot lose the sub-second part the way a seconds count would.
    ///
    /// Fractional digits past the ninth are truncated, so the value never
    /// claims more precision than it has.
    ///
    /// # Errors
    ///
    /// [`RociaDbError::Decode`] with `context` `"file timestamp"`, on the same
    /// terms as [`system_time`](Self::system_time) — bar the platform range,
    /// which an `i128` of nanoseconds cannot run out of for any year this
    /// parser accepts.
    pub fn unix_nanos(&self) -> Result<i128> {
        let (seconds, nanos) = self.unix_instant()?;
        Ok(i128::from(seconds) * 1_000_000_000 + i128::from(nanos))
    }

    /// Parse the raw string into whole seconds since the Unix epoch plus a
    /// nanosecond remainder, mapping the parser's report into the public error.
    /// The one place the two public accessors share, so they cannot disagree
    /// about what is a valid timestamp.
    fn unix_instant(&self) -> Result<(i64, u32)> {
        parse_rfc3339(&self.raw).map_err(|problem| self.decode_error(problem))
    }

    /// The [`RociaDbError::Decode`] both accessors report: what is wrong, and
    /// the value it is wrong about, quoted and bounded by
    /// [`MAX_QUOTED_TIMESTAMP_CHARS`].
    fn decode_error(&self, problem: &str) -> RociaDbError {
        use serde::de::Error as _;

        let quoted: String = self.raw.chars().take(MAX_QUOTED_TIMESTAMP_CHARS).collect();
        let truncated = if quoted.len() < self.raw.len() {
            " (truncated)"
        } else {
            ""
        };
        RociaDbError::Decode {
            context: FILE_TIMESTAMP_CONTEXT,
            source: serde_json::Error::custom(format!("{quoted:?}{truncated}: {problem}")),
        }
    }
}

impl std::fmt::Display for FileTimestamp {
    /// The timestamp exactly as the server wrote it — the same string
    /// [`FileTimestamp::as_str`] returns, parsed or not.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.raw)
    }
}

/// Parse an RFC 3339 timestamp into whole seconds since the Unix epoch and a
/// nanosecond remainder (always positive, even before 1970), or say in a few
/// words why it is not one.
///
/// Pure, allocation-free and dependency-free: the whole of what this SDK knows
/// about the format lives here, so every rule is assertable on its own string
/// without a server, a clock or a date-time crate. The grammar it accepts — and
/// the two decisions it encodes, that a leap second is refused and that an
/// instant before 1970 is not — are documented on [`FileTimestamp`], which is
/// where a caller reads about them.
fn parse_rfc3339(raw: &str) -> std::result::Result<(i64, u32), &'static str> {
    /// Reported for anything whose shape is wrong, as opposed to a field whose
    /// value is out of range: one message, because "which byte of the layout
    /// disagreed" helps nobody read their own timestamp.
    const MISSHAPEN: &str = "expected YYYY-MM-DDThh:mm:ss with a Z or +hh:mm offset";

    let bytes = raw.as_bytes();
    // The date, the separator and the time are a fixed 19 bytes. Checking that
    // much up front is what lets every index below be a constant rather than a
    // bounds check of its own — and stopping at 19 rather than at 20 leaves the
    // offset to `parse_offset`, so the likeliest thing a server that does not
    // speak RFC 3339 sends (a local time, exactly 19 bytes, with no offset at
    // all) is refused by name instead of as a length.
    if bytes.len() < 19 {
        return Err(MISSHAPEN);
    }
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[13] != b':' || bytes[16] != b':' {
        return Err(MISSHAPEN);
    }
    // RFC 3339 §5.6 spells the date/time separator `T`, allows a lower-case
    // `t`, and lets an implementation accept a space in its place (§5.6, NOTE).
    // All three are taken; nothing else is.
    if !matches!(bytes[10], b'T' | b't' | b' ') {
        return Err(MISSHAPEN);
    }
    let year = digits(&bytes[0..4]).ok_or(MISSHAPEN)?;
    let month = digits(&bytes[5..7]).ok_or(MISSHAPEN)?;
    let day = digits(&bytes[8..10]).ok_or(MISSHAPEN)?;
    let hour = digits(&bytes[11..13]).ok_or(MISSHAPEN)?;
    let minute = digits(&bytes[14..16]).ok_or(MISSHAPEN)?;
    let second = digits(&bytes[17..19]).ok_or(MISSHAPEN)?;
    let (nanos, rest) = parse_fraction(&bytes[19..])?;
    let offset_seconds = parse_offset(rest)?;

    if !(1..=12).contains(&month) {
        return Err("month is out of range");
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err("day is out of range for that month");
    }
    if hour > 23 {
        return Err("hour is out of range");
    }
    if minute > 59 {
        return Err("minute is out of range");
    }
    if second > 59 {
        // 60 is a leap second in RFC 3339, and Unix time has no separate
        // instant for one. See `FileTimestamp`.
        return Err("second is out of range (a leap second is not accepted)");
    }

    let seconds = days_from_civil(i64::from(year), month, day) * 86_400
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + i64::from(second)
        - offset_seconds;
    Ok((seconds, nanos))
}

/// Read `bytes` as a decimal number, rejecting anything that is not ASCII
/// digits — a sign, a space, a letter, or any byte of a multi-byte character.
///
/// Called only on slices of at most four bytes, so the accumulator cannot
/// overflow.
fn digits(bytes: &[u8]) -> Option<u32> {
    let mut value = 0;
    for byte in bytes {
        let digit = byte.checked_sub(b'0').filter(|digit| *digit <= 9)?;
        value = value * 10 + u32::from(digit);
    }
    Some(value)
}

/// Split the optional fractional seconds off the front of `rest`, returning the
/// nanoseconds they denote and whatever follows them.
///
/// Any number of digits is accepted. The first nine are kept and the rest are
/// **truncated**, not rounded: nothing finer than a nanosecond survives in a
/// [`SystemTime`] anyway, and rounding would let a value name an instant the
/// server never wrote. Fewer than nine are padded on the right, so `.5` is
/// half a second rather than five nanoseconds.
fn parse_fraction(rest: &[u8]) -> std::result::Result<(u32, &[u8]), &'static str> {
    let Some(tail) = rest.strip_prefix(b".") else {
        return Ok((0, rest));
    };
    let digit_count = tail.iter().take_while(|byte| byte.is_ascii_digit()).count();
    if digit_count == 0 {
        return Err("a decimal point must be followed by at least one digit");
    }
    // Split first: reading nine bytes straight out of `tail` would walk past
    // the fraction and pick digits out of the offset that follows it.
    let (fraction, rest) = tail.split_at(digit_count);
    let mut nanos = 0;
    for index in 0..9 {
        let digit = fraction.get(index).map_or(0, |byte| u32::from(byte - b'0'));
        nanos = nanos * 10 + digit;
    }
    Ok((nanos, rest))
}

/// Read the whole of `rest` as a UTC offset, in seconds to subtract from the
/// local time that preceded it.
///
/// `Z` and `z` are zero. `-00:00`, which RFC 3339 gives the separate meaning
/// "offset unknown", is read as the zero it numerically is — the instant is
/// the same either way, and this type reports instants. Anything left over
/// after the offset is a rejection: a timestamp with trailing text is not one.
fn parse_offset(rest: &[u8]) -> std::result::Result<i64, &'static str> {
    /// Reported for an offset of the wrong shape, including an absent one —
    /// the mistake a server formatting local time without an offset makes.
    const OFFSET: &str = "expected a Z or +hh:mm offset at the end";

    match rest {
        [b'Z' | b'z'] => Ok(0),
        [sign, hour_tens, hour_units, b':', minute_tens, minute_units] => {
            let sign = match sign {
                b'+' => 1,
                b'-' => -1,
                _ => return Err(OFFSET),
            };
            let hours = digits(&[*hour_tens, *hour_units]).ok_or(OFFSET)?;
            let minutes = digits(&[*minute_tens, *minute_units]).ok_or(OFFSET)?;
            if hours > 23 {
                return Err("offset hour is out of range");
            }
            if minutes > 59 {
                return Err("offset minute is out of range");
            }
            Ok(sign * (i64::from(hours) * 3_600 + i64::from(minutes) * 60))
        }
        _ => Err(OFFSET),
    }
}

/// Whether `year` is a leap year in the proleptic Gregorian calendar: every
/// fourth year, except centuries, except every fourth century.
fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

/// How many days `month` has in `year`, or zero for a month number that does
/// not exist — which [`parse_rfc3339`] turns into a rejection, since no day is
/// then in range.
fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Days from the Unix epoch to `year-month-day` in the proleptic Gregorian
/// calendar, negative before 1970-01-01.
///
/// Howard Hinnant's `days_from_civil`: shifting the year to start in March puts
/// the leap day at its end, which is what turns the leap rule into two
/// divisions and removes every table and every special case. Exact for every
/// year this parser accepts (`0000` to `9999`) and far beyond — an era is 400
/// years of exactly 146 097 days — and the 719 468 is the day count from
/// 0000-03-01 to 1970-01-01, which is what makes the result epoch-relative.
///
/// `month` and `day` must already be valid for `year`; [`parse_rfc3339`] checks
/// that before calling this.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let month = i64::from(month);
    // March is the first month of the shifted year, so January and February
    // belong to the year before.
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400; // [0, 399]
    let day_of_year =
        (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + i64::from(day) - 1; // [0, 365]
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year; // [0, 146096]
    era * 146_097 + day_of_era - 719_468
}

/// Turn whole seconds since the Unix epoch plus a nanosecond remainder into a
/// [`SystemTime`], or `None` when this platform cannot represent that instant.
///
/// An instant before 1970 is `UNIX_EPOCH - Duration`, which is why the negative
/// case cannot simply negate: the remainder counts *forward* from the second
/// below it, so subtracting needs one second less and the complement of the
/// nanoseconds. Built from a [`Duration`] of seconds and nanoseconds rather
/// than from nanoseconds alone, which would overflow the `u64`
/// `Duration::from_nanos` takes for any year past 2554. `nanos` is below one
/// second in both branches, so neither `Duration::new` can carry into its
/// seconds — the one way that constructor panics.
fn unix_instant_to_system_time(seconds: i64, nanos: u32) -> Option<SystemTime> {
    if seconds >= 0 {
        return UNIX_EPOCH.checked_add(Duration::new(u64::try_from(seconds).ok()?, nanos));
    }
    let magnitude = if nanos == 0 {
        Duration::new(seconds.unsigned_abs(), 0)
    } else {
        // `seconds` is negative and `nanos` positive, so the magnitude is one
        // whole second less than `|seconds|`, plus the rest of that second.
        Duration::new(seconds.unsigned_abs() - 1, 1_000_000_000 - nanos)
    };
    UNIX_EPOCH.checked_sub(magnitude)
}

/// Options applied to [`RociaDbClient::upload_file`], the in-memory
/// byte-buffer upload.
///
/// There is intentionally no `chunk_size` knob: see
/// [`RociaDbClient::upload_file_stream`] for why 1 MiB is the only size
/// worth using.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileUploadOptions {
    /// MIME type recorded for the file. Defaults to
    /// `"application/octet-stream"`; the server records it as given and never
    /// inspects the bytes to confirm it.
    pub content_type: String,
    /// SHA-256 digest of the uploaded bytes. When `None`,
    /// [`RociaDbClient::upload_file`] computes it from the buffer
    /// automatically — which is almost always what you want; set it
    /// explicitly only when the digest is already known from elsewhere (a
    /// manifest, a previous pass over the same bytes).
    ///
    /// The server checks the length and nothing else: a digest that does
    /// not match the bytes sent produces an upload that looks successful
    /// while recording a checksum the stored file does not satisfy. The
    /// `[u8; 32]` type takes the length half of that off the table at
    /// compile time.
    pub checksum: Option<[u8; 32]>,
    /// Idempotency key for the upload. When `None`, one is generated
    /// automatically (`upload_file:<uuid>` — see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults)). Provide it
    /// explicitly — and reuse the same value on a retry — so an upload
    /// replayed after a timeout is absorbed rather than performed twice.
    pub request_id: Option<String>,
}

impl Default for FileUploadOptions {
    fn default() -> Self {
        Self {
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            checksum: None,
            request_id: None,
        }
    }
}

impl FileUploadOptions {
    /// Options with every field at its default: `"application/octet-stream"`,
    /// a checksum computed from the bytes being uploaded, and a generated
    /// idempotency key.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record `content_type` as the file's MIME type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Send `checksum` instead of computing the SHA-256 digest of the
    /// uploaded bytes; see [`FileUploadOptions::checksum`].
    pub fn with_checksum(mut self, checksum: [u8; 32]) -> Self {
        self.checksum = Some(checksum);
        self
    }

    /// Set the idempotency key for this upload; see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults).
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

/// Options applied to [`RociaDbClient::upload_file_chunked`], the streaming
/// upload.
///
/// Unlike [`FileUploadOptions`], `size_bytes` and `checksum` are required
/// rather than optional, which is why this type has no `Default` and its
/// constructor takes both: the file's metadata travels on the first gRPC
/// message, before a single byte has been read from the caller's stream, so
/// neither value can be derived on the fly the way
/// [`RociaDbClient::upload_file`] derives them from a complete in-memory
/// buffer.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStreamUploadOptions {
    /// Exact total number of bytes the caller's stream will produce. The
    /// upload fails with [`RociaDbError::Validation`] if the stream ends up
    /// shorter or longer, and the server checks the same thing at the end of
    /// the stream.
    pub size_bytes: u64,
    /// SHA-256 digest of the complete file. Hash the source ahead of time (a
    /// first pass over the file, for example): a streaming upload cannot
    /// compute this while sending, since the digest has to be on the first
    /// message. As with [`FileUploadOptions::checksum`], the server checks
    /// the length only.
    pub checksum: [u8; 32],
    /// MIME type recorded for the file. Defaults to
    /// `"application/octet-stream"`; the server records it as given and never
    /// inspects the bytes to confirm it.
    pub content_type: String,
    /// Idempotency key for the upload. When `None`, one is generated
    /// automatically (`upload_file:<uuid>` — see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults)). Provide it
    /// explicitly — and reuse the same value on a retry — so an upload
    /// replayed after a timeout is absorbed rather than performed twice.
    pub request_id: Option<String>,
}

impl FileStreamUploadOptions {
    /// Options for a file of exactly `size_bytes` bytes whose SHA-256 digest
    /// is `checksum`, with `"application/octet-stream"` as the MIME type and
    /// a generated idempotency key.
    pub fn new(size_bytes: u64, checksum: [u8; 32]) -> Self {
        Self {
            size_bytes,
            checksum,
            content_type: DEFAULT_CONTENT_TYPE.to_string(),
            request_id: None,
        }
    }

    /// Record `content_type` as the file's MIME type.
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    /// Set the idempotency key for this upload; see the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults).
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }
}

impl RociaDbClient {
    /// Upload a caller-built stream of protobuf [`UploadRequest`] messages.
    ///
    /// This is a low-level escape hatch for genuine streaming uploads (data
    /// that never fits in memory). The SDK does **not** rechunk or compute
    /// a checksum here — the caller is fully responsible for the wire
    /// contract the server enforces:
    /// - the **first** message must carry `tenant_id`, `bucket`, `file_id`,
    ///   `size_bytes` (the exact total byte count) and `checksum` set to
    ///   the SHA-256 digest of the whole file, as exactly 32 raw bytes;
    /// - every message's `chunk` must not exceed 1 MiB (1_048_576 bytes) —
    ///   below that cap, the server accepts any size, sliced however the
    ///   caller likes;
    /// - `content_type` and `checksum` on messages after the first are
    ///   ignored by the server and can be left empty;
    /// - `request_id`, if any, is read from the first message too; nothing
    ///   is generated for you here, unlike every other write on this client.
    ///
    /// A `chunk` over 1 MiB, a checksum of the wrong length, or a
    /// mismatched `size_bytes` all fail the upload outright with
    /// `INVALID_ARGUMENT` rather than corrupting anything silently. The one
    /// thing the server never verifies is whether `checksum` actually
    /// matches the bytes sent — only that it is 32 bytes long — so a wrong
    /// checksum can still produce an upload that looks successful while
    /// carrying bad data.
    ///
    /// **Why the SDK's own uploads always emit exactly 1 MiB chunks**: that
    /// is the largest message the server allows, so it is also the fewest
    /// possible messages for a given file, and it remains the only chunk
    /// size that is safe against a server older than `1.0.0-rc.16`. Neither
    /// [`RociaDbClient::upload_file`] nor
    /// [`RociaDbClient::upload_file_chunked`] exposes a knob for it.
    ///
    /// For the common case — uploading an in-memory byte buffer — use
    /// [`RociaDbClient::upload_file`] instead, which builds a correct
    /// stream for you; for a large source you cannot buffer but can still
    /// checksum ahead of time, prefer
    /// [`RociaDbClient::upload_file_chunked`], which re-chunks and
    /// validates for you.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// does **not** apply here, nor to any of the upload helpers built on
    /// this method: how long a stream takes is a property of the caller's
    /// own data rate, not of a single round trip. Wrap the call in a
    /// `tokio::time::timeout` of your own if it needs a deadline.
    ///
    /// # Authentication
    ///
    /// With auth enabled, the token is refreshed before the call opens if
    /// little of its lifetime is left, and that is **all** this method gets:
    /// an `UNAUTHENTICATED` answer is returned to you rather than retried.
    /// The reason is `requests`, which this method does not own — it is a
    /// stream the caller handed over, and h2 starts writing body frames onto
    /// the wire as soon as the call opens, so "nothing has been consumed yet"
    /// is not a state the SDK can establish, let alone rely on. Re-sending
    /// would mean draining a stream that has already been partly drained.
    /// Recover by calling
    /// [`refresh_auth_token`](RociaDbClient::refresh_auth_token) and
    /// re-issuing the call with a freshly built stream — and reuse the first
    /// message's `request_id` when you do, so the server recognizes the
    /// replay. [`RociaDbClient::upload_file`] has no such problem (it owns
    /// its buffer) and does retry once for you.
    pub async fn upload_file_stream<S>(&self, requests: S) -> Result<()>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        debug!("uploading a caller-built file stream");
        self.upload_raw(requests)
            .await
            .status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Issue the client-streaming `Upload` RPC and hand back the raw
    /// [`tonic::Status`] on failure.
    ///
    /// The one place the generated file client's `upload` is called, and
    /// therefore the one place that runs the pre-flight token refresh every
    /// upload path gets (see
    /// [`RociaDbClient::refresh_token_before_stream`]).
    /// Unlike a unary RPC — which goes through [`RociaDbClient::unary`] — a
    /// streaming upload gets no per-call deadline, and no replay is possible
    /// from *here*: the stream has been moved in and can only be consumed
    /// once. [`RociaDbClient::upload_file`], which builds its stream from a
    /// buffer it owns, replays by calling this a second time with a freshly
    /// built stream.
    ///
    /// The status is left unmapped because
    /// [`RociaDbClient::upload_file_chunked`] must decide between it and its
    /// own client-side error before either is returned, and
    /// [`RociaDbClient::upload_file`] must inspect it before deciding to
    /// replay.
    async fn upload_raw<S>(&self, requests: S) -> std::result::Result<(), tonic::Status>
    where
        S: Stream<Item = UploadRequest> + Send + 'static,
    {
        self.refresh_token_before_stream(UPLOAD_OPERATION).await;
        let mut upstream_file = self.upstream_file.clone();
        upstream_file
            .upload(requests)
            .await
            .map(tonic::Response::into_inner)
    }

    /// Upload an in-memory byte buffer, split into gRPC messages of the
    /// server's largest allowed chunk size.
    ///
    /// The buffer is always split into 1 MiB (1_048_576-byte) chunks, the
    /// last one possibly shorter; not configurable, see
    /// [`RociaDbClient::upload_file_stream`] for why. When
    /// [`FileUploadOptions::checksum`] is `None`, the SHA-256 digest of
    /// `bytes` is computed and sent automatically. A file over
    /// [`RociaDbBuilder::max_file_bytes`](crate::RociaDbBuilder::max_file_bytes)
    /// (5 GiB by default, mirroring the server's `limits.max_file_bytes`) is
    /// rejected client-side with a [`RociaDbError::Validation`] instead of
    /// failing partway through the upload.
    ///
    /// `bytes` is taken as `impl Into<Vec<u8>>`, so both ownership styles
    /// are one call: a `Vec<u8>` you already hold — a file read off disk, a
    /// buffer assembled in memory — is **moved** straight into the chunking
    /// step with no copy, while a borrowed `&[u8]` (or `&[u8; N]`, or
    /// `&str`) is **copied once** into the owned buffer that the chunking
    /// step and the underlying `'static` upload stream require. That copy is
    /// unavoidable for a borrowed buffer, and worth avoiding for a large
    /// owned one: it doubles peak memory for the whole upload, since Rust's
    /// drop scopes keep the original alive until the upload finishes.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`FileUploadOptions::request_id`] is unset.
    ///
    /// # Authentication
    ///
    /// This is the one upload path that gets the full treatment a unary RPC
    /// gets: the token is refreshed before the call when little of its
    /// lifetime is left, and an `UNAUTHENTICATED` answer triggers one coalesced
    /// refresh and one re-send — never more than one. It can do that because
    /// the buffer belongs to it (it is held in an `Arc`, so the second attempt
    /// re-slices the same bytes rather than copying the file), and because the
    /// re-send carries the
    /// **same** `request_id`: the server deduplicates on it, so a replay that
    /// lands on an upload the server had already committed is absorbed instead
    /// of writing the file twice. [`RociaDbClient::upload_file_chunked`] and
    /// [`RociaDbClient::upload_file_stream`] cannot do this — their source is
    /// a stream, not a buffer — and get the pre-flight refresh only.
    pub async fn upload_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        bytes: impl Into<Vec<u8>>,
        options: FileUploadOptions,
    ) -> Result<()> {
        // An `Arc` rather than a plain `Vec<u8>`: the request stream has to be
        // rebuildable for the replay below, and `Arc::clone` shares the buffer
        // instead of copying a file's worth of bytes for a second attempt that
        // usually never happens.
        let bytes = Arc::new(bytes.into());
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| RociaDbError::validation("file is too large"))?;
        validate_file_size(size_bytes, self.max_file_bytes)?;

        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = size_bytes,
            "uploading file"
        );
        let checksum = resolve_checksum_offloaded(options.checksum, &bytes).await;
        let request_id = options
            .request_id
            .unwrap_or_else(default_upload_file_request_id);

        // One attempt's worth of request stream, built fresh each time it is
        // called. Every attempt sends the same `request_id`, which is what
        // makes the replay safe: the server treats the second attempt as the
        // same write as the first.
        let requests = || {
            stream::iter(chunk_upload_requests(
                tenant_id.to_string(),
                bucket.to_string(),
                file_id.to_string(),
                Arc::clone(&bytes),
                options.content_type.clone(),
                checksum,
                request_id.clone(),
            ))
        };

        let error = match self.upload_raw(requests()).await {
            Ok(()) => return Ok(()),
            Err(status) => RociaDbError::Status {
                operation: UPLOAD_OPERATION,
                status,
            },
        };
        // Exactly the rules `RociaDbClient::unary` follows, from the same
        // helper: replay only on `UNAUTHENTICATED`, only with auth enabled,
        // only once, and return the original error (with a `warn!`) if the
        // refresh itself failed.
        if !self.refresh_for_replay(UPLOAD_OPERATION, &error).await {
            return Err(error);
        }
        self.upload_raw(requests())
            .await
            .status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Upload a stream of arbitrarily-sized byte chunks without buffering
    /// the complete file in memory.
    ///
    /// This is the middle tier between [`RociaDbClient::upload_file`]
    /// (buffers the whole file, computes the checksum for you) and
    /// [`RociaDbClient::upload_file_stream`] (a raw pass-through with zero
    /// validation, and the caller must already match the server's exact
    /// wire contract). `chunks` may be split however the source naturally
    /// produces data — a 64 KiB `AsyncRead` wrapper, protobuf messages
    /// from another stream, anything — this method re-buffers internally
    /// and always emits exactly-1-MiB gRPC messages to the server (the last
    /// one may be shorter), the same chunking [`RociaDbClient::upload_file`]
    /// produces from an in-memory buffer. It never holds more than one
    /// outgoing chunk's worth of bytes at a time, however `chunks` happens
    /// to be sliced.
    ///
    /// # The item type
    ///
    /// `std::io::Result<Bytes>` is chosen so the obvious source needs no
    /// adaptation at all: `tokio_util::io::ReaderStream` wraps any
    /// `tokio::io::AsyncRead` — a `tokio::fs::File`, a socket, a decompressor
    /// — and yields exactly this item, so it is passed straight in.
    ///
    /// ```rust,no_run
    /// # use rociadb_sdk::{FileStreamUploadOptions, RociaDbBuilder};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = RociaDbBuilder::new().disable_auth().build().await?;
    /// # let (size_bytes, checksum) = (0u64, [0u8; 32]);
    /// let file = tokio::fs::File::open("large-report.csv").await?;
    /// client
    ///     .upload_file_chunked(
    ///         "tenant-1",
    ///         "reports",
    ///         "large-report.csv",
    ///         tokio_util::io::ReaderStream::new(file),
    ///         FileStreamUploadOptions::new(size_bytes, checksum),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// The `Result` is the load-bearing half: a read that fails partway
    /// through is an `Err` item, and it **fails the upload** with
    /// [`RociaDbError::Io`] carrying the [`std::io::Error`]. An item type of
    /// plain bytes would leave a failing source no way to say so — it could
    /// only end early, and the upload would be reported as a size mismatch,
    /// blaming the caller's `size_bytes` for a disk that could not be read.
    ///
    /// **"Partway through" is the condition, and it is exact.** A source that
    /// fails once every one of the declared `size_bytes` has been *read* has
    /// failed after the last byte this upload needed: the upload completes, the
    /// call returns `Ok(())`, and the failure is logged at `warn!` rather than
    /// returned. This is not an edge case to be surprised by — the source is
    /// deliberately polled one item past the declared total, because that is how
    /// an over-declared upload is caught, so a source that ends with an error
    /// right after its last byte is the ordinary shape of it.
    ///
    /// One exception, for a `size_bytes` of zero: a source that fails there is
    /// always reported, never forgiven. Nothing was read, so the failure says
    /// nothing about the file's contents — but publishing *replaces* whatever is
    /// already stored under `file_id`, so forgiving it would let a size
    /// computation that wrongly returned zero destroy a stored file and report
    /// success.
    ///
    /// Nothing is pulled from `chunks` after an `Err`, and the [`Bytes`] items
    /// are copied into the outgoing chunk buffer like any other bytes: the
    /// type is there for what it makes easy at the call site, not to make the
    /// upload zero-copy.
    ///
    /// # Validation and errors
    ///
    /// [`FileStreamUploadOptions::size_bytes`] must be the exact total the
    /// caller intends to send and [`FileStreamUploadOptions::checksum`] the
    /// SHA-256 digest of the complete file; see that type for why neither
    /// can be computed here. If `chunks` ends up producing more or fewer
    /// total bytes than `size_bytes` declared, this fails with
    /// [`RociaDbError::Validation`] instead of silently sending a
    /// corrupt-on-download file: the server itself also checks this at the
    /// end of the stream, but catching it here gives a clearer, immediate
    /// error naming the actual byte counts involved.
    ///
    /// Both client-side failures — the [`RociaDbError::Io`] from a failing
    /// source and the [`RociaDbError::Validation`] from a byte count that
    /// disagrees — take precedence over whatever status the server returned
    /// for the stream that then ended early, because they say what actually
    /// went wrong.
    ///
    /// A declared `size_bytes` over
    /// [`RociaDbBuilder::max_file_bytes`](crate::RociaDbBuilder::max_file_bytes)
    /// (5 GiB by default) is rejected before the call opens, exactly as in
    /// [`RociaDbClient::upload_file`] — here it costs nothing to notice, since
    /// the total is declared up front rather than measured.
    ///
    /// # Authentication
    ///
    /// With auth enabled the token is refreshed before the call opens if
    /// little of its lifetime is left, and that is all: an `UNAUTHENTICATED`
    /// answer is returned rather than retried, because `chunks` is the
    /// caller's and h2 starts writing body frames as soon as the call opens —
    /// so "nothing has been consumed yet" is not a state the SDK can
    /// establish, and a replay would have to re-drain a stream that is already
    /// partly drained. Recover by calling
    /// [`refresh_auth_token`](RociaDbClient::refresh_auth_token) and
    /// re-issuing with a fresh `chunks`, reusing the same
    /// [`FileStreamUploadOptions::request_id`] so the server recognizes the
    /// replay. [`RociaDbClient::upload_file`] owns its buffer and does retry
    /// once on its own.
    ///
    /// **Naming note**: despite matching the server's chunking contract,
    /// this is not called `upload_file_stream` — that name belongs to the
    /// raw, zero-validation escape hatch above it.
    pub async fn upload_file_chunked<S>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        chunks: S,
        options: FileStreamUploadOptions,
    ) -> Result<()>
    where
        S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
    {
        validate_file_size(options.size_bytes, self.max_file_bytes)?;
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = options.size_bytes,
            "uploading file from a chunk stream"
        );
        let request_id = options
            .request_id
            .unwrap_or_else(default_upload_file_request_id);

        // Set by `rechunk_upload_requests` when the source failed with an I/O
        // error before the declared total had been sent, or produced a total
        // byte count that does not match `size_bytes`, since the outgoing
        // `Stream<Item = UploadRequest>` itself has no channel to carry an
        // error — it can only end early. Checked below regardless of whether
        // the RPC itself succeeded or failed, so this client-side error takes
        // precedence over whatever the server made of a stream that ended up
        // short or truncated.
        //
        // A source that fails *after* its last declared byte records nothing:
        // the server has a complete stream and its verdict is the honest
        // answer. See the `Some(Err(..))` arm of `rechunk_upload_requests`.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests = rechunk_upload_requests(
            tenant_id.to_string(),
            bucket.to_string(),
            file_id.to_string(),
            options.size_bytes,
            options.content_type,
            options.checksum,
            request_id,
            chunks,
            Arc::clone(&error_slot),
        );

        let upload_result = self.upload_raw(requests).await;
        if let Some(error) = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            return Err(error);
        }
        upload_result.status_context(UPLOAD_OPERATION)?;
        Ok(())
    }

    /// Start a server-streaming download without buffering the complete file.
    ///
    /// This performs no integrity verification of its own — it is a thin
    /// wrapper that opens the raw gRPC download stream and hands it back
    /// as-is. That is a real asymmetry with the upload path: every upload
    /// method on this client sends a SHA-256 checksum with the file (and
    /// [`RociaDbClient::upload_file`] computes it for you), and
    /// [`FileMetadata::checksum`] exposes the checksum recorded for a stored
    /// file — but nothing on the download side ever computes or checks a
    /// checksum against the chunks the server sends back, and the server
    /// does not send one on download for this method to check. The only
    /// protection you get is whatever the transport itself already provides
    /// (TLS and HTTP/2 framing catch corruption or truncation in transit),
    /// which says nothing about whether the bytes stored on the server still
    /// match what was originally uploaded.
    ///
    /// Two methods close that gap, both by stating first, hashing while they
    /// stream, and failing rather than handing over bytes that disagree with
    /// the metadata: [`RociaDbClient::download_file_verified`] for a file that
    /// fits in memory, and [`RociaDbClient::download_file_verified_to`] for one
    /// that does not — it writes into a `tokio::io::AsyncWrite` of your choice
    /// and never buffers more than a chunk. Use this raw stream when you need
    /// the chunks themselves; verifying it by hand then means hashing each one
    /// as it arrives and comparing the digest against
    /// [`RociaDbClient::stat_file`]'s `checksum` at the end.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// does **not** apply here, nor to [`RociaDbClient::download_file`]: how
    /// long a transfer takes is a property of the file's size and the link,
    /// not of a single round trip. Wrap the call in a `tokio::time::timeout`
    /// of your own if it needs a deadline.
    ///
    /// # Authentication
    ///
    /// The call that *opens* the stream is covered like a unary RPC: the token
    /// is refreshed first if little of its lifetime is left, and an
    /// `UNAUTHENTICATED` rejection triggers one coalesced refresh and one
    /// re-issue. That works here — unlike for a streaming upload — because a
    /// server that rejects the call does so before any message exists, so this
    /// future resolves with the status and there is nothing consumed to replay
    /// around. Nothing covers the stream *after* it opens: the server checked
    /// the bearer token once, when it accepted the call, so a long transfer is
    /// not interrupted by its token expiring, but a status that arrives in the
    /// stream's trailers reaches you through
    /// [`Streaming::message`](tonic::codec::Streaming::message) unretried.
    pub async fn download_file_stream(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Streaming<DownloadResponse>> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "downloading file"
        );
        self.refresh_token_before_stream(DOWNLOAD_OPERATION).await;
        let request = DownloadRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
        };
        // `server_streaming` rather than `unary`: the same refresh-and-retry,
        // deliberately without the per-RPC deadline. See its documentation for
        // what a `grpc-timeout` header would do to a server-streaming call.
        self.server_streaming(DOWNLOAD_OPERATION, request, |request| {
            let mut upstream = self.upstream_file.clone();
            async move { upstream.download(request).await }
        })
        .await
    }

    /// Download a complete file into memory.
    ///
    /// Collects every chunk from [`RociaDbClient::download_file_stream`]
    /// into one buffer via `extend_from_slice` and nothing else — see that
    /// method's docs for the full asymmetry with the upload path: no
    /// checksum is computed or checked here either, so a file that was
    /// corrupted or truncated in storage is still returned successfully,
    /// with its bad bytes intact and no error raised. Use
    /// [`RociaDbClient::download_file_verified`] when the bytes have to be
    /// checked against the metadata the server recorded for them, or
    /// [`RociaDbClient::download_file_verified_to`] when they also must not be
    /// buffered.
    ///
    /// # Nothing bounds what this allocates
    ///
    /// The buffer grows for as long as the server keeps sending, and this
    /// method never asks how big the file is: there is no
    /// [`stat_file`](RociaDbClient::stat_file) first, no ceiling to configure,
    /// and [`max_file_bytes`](crate::RociaDbBuilder::max_file_bytes) gates
    /// uploads only. Calling it on a file whose size you do not control — or
    /// against an endpoint you do not control — is handing that endpoint your
    /// process's memory.
    ///
    /// The three alternatives are each bounded, and one of them is almost
    /// always what you want for a file of unknown size:
    ///
    /// - [`download_file_verified`](RociaDbClient::download_file_verified)
    ///   stats first and stops the moment the bytes received exceed the size
    ///   the server reported, so a server sending more than it declared is cut
    ///   off rather than buffered; its up-front reservation is capped
    ///   independently of that figure.
    /// - [`download_file_verified_to`](RociaDbClient::download_file_verified_to)
    ///   holds one chunk at a time and writes the rest out to a writer of
    ///   yours.
    /// - [`download_file_stream`](RociaDbClient::download_file_stream) hands
    ///   you the chunks and lets you decide.
    pub async fn download_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Vec<u8>> {
        let mut stream = self
            .download_file_stream(tenant_id, bucket, file_id)
            .await?;
        let mut bytes = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .status_context("file download stream failed")?
        {
            bytes.extend_from_slice(&response.chunk);
        }
        Ok(bytes)
    }

    /// Download a complete file into memory and check it against the
    /// metadata the server recorded for it.
    ///
    /// The verifying counterpart of [`RociaDbClient::download_file`]. It
    /// calls [`RociaDbClient::stat_file`] first, streams the download while
    /// feeding every chunk to a SHA-256 hasher, and only then hands the
    /// buffer back — after checking both the byte count against
    /// [`FileMetadata::size_bytes`] and the digest against
    /// [`FileMetadata::checksum`]. A file whose stored bytes have been
    /// corrupted or truncated therefore fails here instead of being returned
    /// as if nothing were wrong.
    ///
    /// [`RociaDbClient::download_file_verified_to`] applies exactly the same
    /// checks without ever buffering the file: use it for anything whose size
    /// you are not prepared to hold in memory.
    ///
    /// # What this proves, and what it does not
    ///
    /// **The server never verified the uploader's checksum.** It checks that
    /// the value is 32 bytes long and stores it; it never hashes the bytes it
    /// received to confirm the two agree (see
    /// [`FileMetadata::checksum`] and
    /// [`RociaDbClient::upload_file_stream`]). So a match here proves the
    /// bytes you just received are the bytes the uploader *declared* — it
    /// catches storage corruption, a truncated transfer, and a partially
    /// overwritten file, and it does not catch an uploader that sent a
    /// checksum which never matched its own payload. End-to-end integrity
    /// against a source you do not control needs a digest carried out of
    /// band, not this.
    ///
    /// Nothing is checked atomically with the download either: `stat_file`
    /// and the download are two calls, so a file replaced between them is
    /// read as a mismatch rather than as the new version. Replacement is
    /// atomic server-side (see [`UploadRequest::file_id`](crate::UploadRequest)),
    /// so the mismatch is the worst case — never a mixed file.
    ///
    /// # Errors
    ///
    /// [`RociaDbError::SizeMismatch`] when the byte count disagrees with
    /// `size_bytes` (checked first: a truncated stream fails both checks, and
    /// the byte count is the more actionable report), then
    /// [`RociaDbError::ChecksumMismatch`] when the digest disagrees with the
    /// stored checksum — including when that checksum is not a 32-byte
    /// SHA-256 digest at all, since the comparison is over raw bytes.
    /// Otherwise whatever [`RociaDbClient::stat_file`] and
    /// [`RociaDbClient::download_file_stream`] return: `NOT_FOUND` for an
    /// unknown `file_id`, and so on.
    ///
    /// # Memory
    ///
    /// The whole file is buffered, like [`RociaDbClient::download_file`]. The
    /// buffer is pre-allocated from the size `stat_file` reported, capped at
    /// 64 MiB, so a server reporting an absurd `size_bytes` cannot make the
    /// client reserve gigabytes before a single byte has arrived; a genuinely
    /// larger file simply grows the buffer as it streams, exactly as
    /// `download_file` does. [`RociaDbClient::download_file_verified_to`]
    /// removes the buffer altogether.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// covers the `stat_file` call (a unary RPC) but not the download stream,
    /// as everywhere else on this client.
    pub async fn download_file_verified(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<Vec<u8>> {
        let metadata = self.stat_file(tenant_id, bucket, file_id).await?;
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = metadata.size_bytes,
            "downloading file with verification"
        );
        // A `Vec<u8>` is itself a `tokio::io::AsyncWrite` (writing to it is an
        // infallible `extend_from_slice`), so the buffered variant is the
        // streaming one pointed at a buffer — the capped pre-allocation still
        // happens here, because the writer variant has nothing to pre-allocate.
        let mut bytes = Vec::with_capacity(preallocated_download_capacity(metadata.size_bytes));
        self.download_verified_into(tenant_id, bucket, file_id, &metadata, &mut bytes)
            .await?;
        Ok(bytes)
    }

    /// Download a file straight into `writer`, checking it against the
    /// metadata the server recorded for it, without ever buffering it.
    ///
    /// Same checks as [`RociaDbClient::download_file_verified`] — the same
    /// [`RociaDbClient::stat_file`] call first, the same SHA-256 fed chunk by
    /// chunk, the same byte count against [`FileMetadata::size_bytes`] and the
    /// same digest against [`FileMetadata::checksum`] — but nothing is kept:
    /// each chunk is hashed and written out as it arrives, so a 5 GiB file
    /// costs one chunk of memory rather than 5 GiB. Returns the number of bytes
    /// written, which on success is necessarily
    /// [`FileMetadata::size_bytes`]. `writer` is flushed once, after every
    /// check has passed.
    ///
    /// `W` is `?Sized`, so a `&mut dyn AsyncWrite + Unpin` works as well as a
    /// concrete `tokio::fs::File`, a `tokio::io::BufWriter`, a socket or a
    /// `Vec<u8>`. It is a separate method rather than an option on
    /// [`RociaDbClient::download_file_verified`] because the two differ in what
    /// they *return* as much as in where they write, and a borrowed writer is
    /// not something an options struct can carry — the same reason the three
    /// upload tiers are three methods.
    ///
    /// ```rust,no_run
    /// # use rociadb_sdk::RociaDbBuilder;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let client = RociaDbBuilder::new().disable_auth().build().await?;
    /// // Download to a temporary path, and publish it only once it verifies.
    /// let mut file = tokio::fs::File::create("report.csv.part").await?;
    /// match client
    ///     .download_file_verified_to("tenant-1", "reports", "report.csv", &mut file)
    ///     .await
    /// {
    ///     Ok(bytes) => {
    ///         tokio::fs::rename("report.csv.part", "report.csv").await?;
    ///         println!("{bytes} verified bytes");
    ///     }
    ///     Err(error) => {
    ///         // The partial file is this caller's to clean up; see below.
    ///         tokio::fs::remove_file("report.csv.part").await?;
    ///         return Err(error.into());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # A failed verification has already written bytes
    ///
    /// **Verification can only fail at the end**, because the digest of a file
    /// is not known until its last byte has arrived — and a size check cannot
    /// come earlier either. So when this returns
    /// [`RociaDbError::SizeMismatch`] or [`RociaDbError::ChecksumMismatch`],
    /// `writer` has already received most or all of the file, and **discarding
    /// it is the caller's job**: delete the temporary file, roll back the
    /// transaction, truncate the buffer. Writing to the destination path
    /// directly makes a failure overwrite good data with bad; download to a
    /// temporary name and rename it once this returns `Ok`, as above.
    /// [`RociaDbClient::download_file_verified`] does not have this property —
    /// it owns the buffer it would throw away — and is the better choice
    /// whenever the file does fit in memory.
    ///
    /// # What this proves, and what it does not
    ///
    /// Exactly what [`RociaDbClient::download_file_verified`] proves, and no
    /// more: the server never verified the uploader's checksum, so a match
    /// shows the bytes received are the bytes the uploader *declared*. It
    /// catches storage corruption, a truncated transfer and a partially
    /// overwritten file; it does not catch an uploader whose declared digest
    /// never matched its own payload. `stat_file` and the download are still
    /// two calls, so a file replaced between them reads as a mismatch rather
    /// than as the new version. See
    /// [`download_file_verified`](RociaDbClient::download_file_verified) for the
    /// full discussion.
    ///
    /// # Errors
    ///
    /// [`RociaDbError::Io`] with `context` `"writing the downloaded file"` when
    /// `writer` refuses a chunk or the final flush fails — the download is
    /// abandoned at that point and nothing further is read from the stream.
    /// Then [`RociaDbError::SizeMismatch`] and
    /// [`RociaDbError::ChecksumMismatch`] on the same terms, and in the same
    /// order, as [`RociaDbClient::download_file_verified`]: the size first,
    /// including as soon as the stream overshoots it mid-transfer (that chunk
    /// is not written). Otherwise whatever `stat_file` and
    /// [`RociaDbClient::download_file_stream`] return, `NOT_FOUND` for an
    /// unknown `file_id` included.
    ///
    /// # Authentication
    ///
    /// As for every download: the token is refreshed before the stream opens if
    /// little of its lifetime is left, and an `UNAUTHENTICATED` rejection of the
    /// *opening* call triggers one coalesced refresh and one re-issue. Both come
    /// from [`RociaDbClient::download_file_stream`], which this uses.
    ///
    /// [`RociaDbBuilder::request_timeout`](crate::RociaDbBuilder::request_timeout)
    /// covers the `stat_file` call (a unary RPC) but not the download stream.
    /// Bound the transfer with a `tokio::time::timeout` of your own if it needs
    /// a deadline.
    pub async fn download_file_verified_to<W>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        writer: &mut W,
    ) -> Result<u64>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        let metadata = self.stat_file(tenant_id, bucket, file_id).await?;
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            size_bytes = metadata.size_bytes,
            "downloading file with verification into a caller-supplied writer"
        );
        self.download_verified_into(tenant_id, bucket, file_id, &metadata, writer)
            .await
    }

    /// Stream one download into `writer`, verifying it against `metadata`, and
    /// report how many bytes it carried.
    ///
    /// The whole of what the two verified downloads share, so the rules —
    /// hash every chunk as it arrives, stop on an overshoot, check the size
    /// before the digest, flush only once both agree — exist once.
    /// [`RociaDbClient::download_file_verified`] passes a `Vec<u8>` it
    /// pre-allocated; [`RociaDbClient::download_file_verified_to`] passes the
    /// caller's writer. `metadata` is taken by reference and never re-read
    /// here: both callers have already issued the [`RociaDbClient::stat_file`]
    /// that produced it, and issuing it again would make the verification
    /// compare against metadata the download did not start from.
    async fn download_verified_into<W>(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        metadata: &FileMetadata,
        writer: &mut W,
    ) -> Result<u64>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        let mut verified = VerifiedDownload::new(metadata.size_bytes);
        let mut stream = self
            .download_file_stream(tenant_id, bucket, file_id)
            .await?;
        while let Some(response) = stream
            .message()
            .await
            .status_context("file download stream failed")?
        {
            verified.absorb(writer, &response.chunk).await?;
        }
        let written = verified.finish(&metadata.checksum)?;
        // Only now: a caller is going to discard whatever a failed
        // verification wrote, so there is nothing worth pushing out of the
        // writer's own buffers before the checks have passed.
        writer.flush().await.map_err(download_write_error)?;
        Ok(written)
    }

    /// Return metadata for one stored file: its size, its recorded MIME type
    /// and checksum, and when it was first and last written.
    ///
    /// Reads the **published** version, so an upload still in flight is
    /// `NOT_FOUND` here until its stream has been received and validated in
    /// full. It is also the call the two verified downloads make first, and the
    /// way to find out whether a file exists at all — [`delete_file`] being
    /// idempotent, it cannot tell you.
    ///
    /// The two timestamps are [`FileTimestamp`]s: the server's own string,
    /// available verbatim through [`FileTimestamp::as_str`], and parsed into a
    /// [`SystemTime`] only if you ask for one. See that type for the format it
    /// reads and what happens when a server writes another.
    ///
    /// # Errors
    ///
    /// `NOT_FOUND` for a `file_id` that is not published in this bucket, and
    /// whatever else the server reports. Nothing about the response is
    /// validated client-side: a timestamp only fails when a caller asks it to
    /// parse, and the `checksum` is compared only by the two verified
    /// downloads.
    ///
    /// [`delete_file`]: RociaDbClient::delete_file
    pub async fn stat_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
    ) -> Result<FileMetadata> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "reading file metadata"
        );
        let request = StatRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
        };
        let response: StatResponse = self
            .unary("failed to stat file", request, |request| {
                let mut upstream = self.upstream_file.clone();
                async move { upstream.stat(request).await }
            })
            .await?;
        Ok(FileMetadata::from(response))
    }

    /// Return one paginated page of bucket names holding at least one file.
    pub async fn list_buckets(
        &self,
        tenant_id: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing buckets"
        );
        let request = ListBucketsRequest {
            tenant_id: tenant_id.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list buckets", request, |request| {
                let mut upstream = self.upstream_file.clone();
                async move { upstream.list_buckets(request).await }
            })
            .await?;
        Ok(Page {
            items: response.buckets,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Return one paginated page of file ids stored in one bucket.
    pub async fn list_files(
        &self,
        tenant_id: &str,
        bucket: &str,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<String>> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            limit = limit.unwrap_or(DEFAULT_PAGE_SIZE),
            cursor = cursor.unwrap_or(""),
            "listing files"
        );
        let request = ListFilesRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            page: page_request(limit, cursor)?,
        };
        let response = self
            .unary("failed to list files", request, |request| {
                let mut upstream = self.upstream_file.clone();
                async move { upstream.list_files(request).await }
            })
            .await?;
        Ok(Page {
            items: response.file_ids,
            next_cursor: response.page.and_then(|page| non_empty(page.next_cursor)),
        })
    }

    /// Delete one stored file.
    ///
    /// **Idempotent**, like [`RociaDbClient::delete_document`] and
    /// [`RociaDbClient::delete_edge`]: deleting a `file_id` that does not
    /// exist succeeds and touches nothing. Call
    /// [`RociaDbClient::stat_file`] first when you need to know whether the
    /// file was there.
    ///
    /// See the [idempotency key
    /// defaults](WriteOptions#idempotency-key-defaults) for the key used
    /// when [`WriteOptions::request_id`] is unset.
    pub async fn delete_file(
        &self,
        tenant_id: &str,
        bucket: &str,
        file_id: &str,
        options: WriteOptions,
    ) -> Result<()> {
        debug!(
            tenant_id = tenant_id,
            bucket = bucket,
            file_id = file_id,
            "deleting file"
        );
        let request = DeleteRequest {
            tenant_id: tenant_id.to_string(),
            bucket: bucket.to_string(),
            file_id: file_id.to_string(),
            request_id: options
                .request_id
                .unwrap_or_else(|| format!("delete_file:{}", Uuid::new_v4())),
        };
        self.unary("failed to delete file", request, |request| {
            let mut upstream = self.upstream_file.clone();
            async move { upstream.delete(request).await }
        })
        .await?;
        Ok(())
    }
}

/// Default idempotency key for an `Upload` call, shared by
/// [`RociaDbClient::upload_file`] and
/// [`RociaDbClient::upload_file_chunked`] so the prefix never depends on
/// which of the two produced the upload.
fn default_upload_file_request_id() -> String {
    format!("upload_file:{}", Uuid::new_v4())
}

/// Resolve the checksum to send: the caller's when they supplied one, or the
/// SHA-256 digest of `bytes` computed here. Pure and network-free — and
/// infallible, since `[u8; 32]` makes the length the compiler's business
/// rather than a runtime check.
fn resolve_checksum(checksum: Option<[u8; 32]>, bytes: &[u8]) -> [u8; 32] {
    checksum.unwrap_or_else(|| Sha256::digest(bytes).into())
}

/// Buffer size above which [`resolve_checksum_offloaded`] hashes on the
/// blocking pool instead of inline.
///
/// One chunk — the unit the upload already works in, so this introduces no new
/// number to reason about. Below it the digest is a matter of microseconds and a
/// thread-pool hop would cost more than it saves.
const INLINE_CHECKSUM_LIMIT: usize = DEFAULT_CHUNK_SIZE;

/// [`resolve_checksum`], moved off the runtime thread when the buffer is large
/// enough for that to matter.
///
/// SHA-256 runs at a few hundred MiB/s, so hashing inline costs a
/// multi-gigabyte [`RociaDbClient::upload_file`] whole seconds inside
/// `Sha256::digest` with no await point in them. That starves every other task
/// on the worker — on a current-thread runtime, every task the program has — and
/// [`RociaDbBuilder::max_file_bytes`](crate::RociaDbBuilder::max_file_bytes)
/// defaults to 5 GiB, so this is a size the SDK invites.
///
/// A caller-supplied digest and a small buffer both stay on the current thread,
/// which is every case where the hop would be the more expensive half.
///
/// A `JoinError` can only mean the runtime is shutting down — the digest itself
/// has no way to panic — and hashing inline is a better answer to that than a
/// new error variant for something the caller cannot act on: the work is pure,
/// short-lived and idempotent, so repeating it costs only the time it takes.
///
/// One consequence worth knowing: a blocking task cannot be cancelled. Dropping
/// the [`RociaDbClient::upload_file`] future mid-hash — a `tokio::time::timeout`
/// firing, say — leaves the digest running to completion on a pool thread,
/// holding its `Arc` on the buffer until it finishes. Not a regression, since
/// the inline version ran to completion inside a single poll and could not be
/// cancelled either; the difference is only that the thread is now someone
/// else's to wait for.
async fn resolve_checksum_offloaded(checksum: Option<[u8; 32]>, bytes: &Arc<Vec<u8>>) -> [u8; 32] {
    if let Some(checksum) = checksum {
        return checksum;
    }
    if bytes.len() <= INLINE_CHECKSUM_LIMIT {
        return resolve_checksum(None, bytes);
    }
    let owned = Arc::clone(bytes);
    tokio::task::spawn_blocking(move || resolve_checksum(None, &owned))
        .await
        .unwrap_or_else(|_| resolve_checksum(None, bytes))
}

/// Validate that `size_bytes` does not exceed `max_file_bytes`, before any
/// network call. Shared by [`RociaDbClient::upload_file`] and
/// [`RociaDbClient::upload_file_chunked`] so both reject an oversized file
/// with the same client-side error instead of letting the upload run and
/// fail server-side partway through.
///
/// `max_file_bytes` is a parameter rather than a read of
/// [`DEFAULT_MAX_FILE_BYTES`] because it is configurable per client (see
/// [`RociaDbBuilder::max_file_bytes`](crate::RociaDbBuilder::max_file_bytes));
/// keeping the rule in a pure function is what lets both the default and a
/// lowered limit be asserted without a socket.
fn validate_file_size(size_bytes: u64, max_file_bytes: u64) -> Result<()> {
    if size_bytes > max_file_bytes {
        return Err(RociaDbError::validation(format!(
            "file is {size_bytes} bytes, which exceeds the client's {max_file_bytes}-byte \
             max_file_bytes limit"
        )));
    }
    Ok(())
}

/// Capacity [`RociaDbClient::download_file_verified`] reserves up front for a
/// file the server reported as `size_bytes` bytes: that size, capped at
/// [`MAX_PREALLOCATED_DOWNLOAD_BYTES`].
///
/// Never trust `size_bytes` with an allocation: it is a number the server
/// chose, and `Vec::with_capacity` would reserve all of it before a byte has
/// arrived. Pure and network-free so the cap can be asserted on its own.
fn preallocated_download_capacity(size_bytes: u64) -> usize {
    usize::try_from(size_bytes.min(MAX_PREALLOCATED_DOWNLOAD_BYTES)).unwrap_or(usize::MAX)
}

/// Map a write of downloaded bytes that the caller's writer refused into
/// [`RociaDbError::Io`], the variant for a failure in something the *caller*
/// handed over rather than in the server or the transport.
fn download_write_error(source: std::io::Error) -> RociaDbError {
    RociaDbError::Io {
        context: DOWNLOAD_WRITE_CONTEXT,
        source,
    }
}

/// The running state of a verified download, and every rule the two verified
/// downloads apply to one.
///
/// [`RociaDbClient::download_file_verified`] and
/// [`RociaDbClient::download_file_verified_to`] differ only in *where* the
/// bytes go — an owned `Vec<u8>` or the caller's writer — so the hashing, the
/// byte counting and both mismatch checks live here once rather than twice.
/// [`VerifiedDownload::record`] and [`VerifiedDownload::finish`] are pure: no
/// socket, no writer, no runtime, so each rule is unit-testable on its own.
/// [`VerifiedDownload::absorb`] is the one step that has to be `async`, and
/// only because writing a chunk out is.
struct VerifiedDownload {
    /// The `size_bytes` [`RociaDbClient::stat_file`] reported: what the chunks
    /// have to add up to, exactly.
    expected_size: u64,
    /// Fed each chunk as it arrives. Hashing an assembled buffer afterwards
    /// would read the whole file a second time, and is not possible at all when
    /// the bytes are handed to a writer and never kept.
    hasher: Sha256,
    /// Bytes accounted for so far, which on success is also the number
    /// [`RociaDbClient::download_file_verified_to`] returns.
    written: u64,
}

impl VerifiedDownload {
    /// Begin verifying a download whose metadata reports `expected_size` bytes.
    fn new(expected_size: u64) -> Self {
        Self {
            expected_size,
            hasher: Sha256::new(),
            written: 0,
        }
    }

    /// Hash and count `chunk`, failing the moment the running total overshoots
    /// `expected_size`.
    ///
    /// Stopping here rather than at the end of the stream is what keeps a
    /// server that sends more than it promised from being drained into memory —
    /// or onto the caller's disk — only to be rejected afterwards. The outcome
    /// is the same [`RociaDbError::SizeMismatch`] either way, and `actual`
    /// still counts the chunk that overshot.
    fn record(&mut self, chunk: &[u8]) -> Result<()> {
        self.hasher.update(chunk);
        self.written = self.written.saturating_add(chunk.len() as u64);
        if self.written > self.expected_size {
            return Err(RociaDbError::SizeMismatch {
                expected: self.expected_size,
                actual: self.written,
            });
        }
        Ok(())
    }

    /// [`VerifiedDownload::record`] `chunk`, then write it to `writer` — in
    /// that order, so a chunk that overshoots `expected_size` is never handed
    /// to the writer at all.
    async fn absorb<W>(&mut self, writer: &mut W, chunk: &[u8]) -> Result<()>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        self.record(chunk)?;
        writer.write_all(chunk).await.map_err(download_write_error)
    }

    /// Check the finished download against the metadata and report how many
    /// bytes it carried.
    ///
    /// The size is checked before the digest: a truncated stream fails both,
    /// and the byte count is the more actionable of the two reports.
    /// `expected_checksum` is compared as raw bytes, so a stored value that is
    /// not a 32-byte SHA-256 digest at all simply cannot match.
    fn finish(self, expected_checksum: &[u8]) -> Result<u64> {
        if self.written != self.expected_size {
            return Err(RociaDbError::SizeMismatch {
                expected: self.expected_size,
                actual: self.written,
            });
        }
        let digest: [u8; 32] = self.hasher.finalize().into();
        if digest.as_slice() != expected_checksum {
            return Err(RociaDbError::ChecksumMismatch {
                expected: expected_checksum.to_vec(),
                actual: digest.to_vec(),
            });
        }
        Ok(self.written)
    }
}

/// Lazily build the per-chunk `UploadRequest` sequence for `bytes`.
///
/// Only the first request carries the file metadata (`tenant_id`,
/// `bucket`, `file_id`, `size_bytes`, `content_type`, `checksum`,
/// `request_id`): the server only reads those fields off the first message
/// of the stream (see [`RociaDbClient::upload_file_stream`]), so building
/// them for every chunk would just be wasted clones. Requests are produced
/// on demand as the returned iterator is polled by the outgoing stream,
/// never collected into a `Vec` up front.
///
/// `checksum` is `[u8; 32]` up to here and becomes a `Vec<u8>` only at the
/// wire boundary, where the protobuf field demands one.
///
/// `bytes` is an `Arc` so [`RociaDbClient::upload_file`] can build this
/// sequence twice — once per attempt of its refresh-and-replay — without
/// copying the file. Each chunk is still copied into its own `Vec<u8>`, because
/// that is what the protobuf field is.
fn chunk_upload_requests(
    tenant_id: String,
    bucket: String,
    file_id: String,
    bytes: Arc<Vec<u8>>,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
) -> impl Iterator<Item = UploadRequest> {
    let size_bytes = bytes.len() as u64;
    // A zero-byte file still needs one message to carry the metadata, even
    // though it has no chunk to store.
    let chunk_count = if bytes.is_empty() {
        1
    } else {
        size_bytes.div_ceil(DEFAULT_CHUNK_SIZE as u64)
    };

    let mut tenant_id = Some(tenant_id);
    let mut bucket = Some(bucket);
    let mut file_id = Some(file_id);
    let mut content_type = Some(content_type);
    let mut checksum = Some(checksum);
    let mut request_id = Some(request_id);

    (0..chunk_count).map(move |index| {
        let start = index as usize * DEFAULT_CHUNK_SIZE;
        let end = (start + DEFAULT_CHUNK_SIZE).min(bytes.len());
        UploadRequest {
            tenant_id: tenant_id.take().unwrap_or_default(),
            bucket: bucket.take().unwrap_or_default(),
            file_id: file_id.take().unwrap_or_default(),
            size_bytes: if index == 0 { size_bytes } else { 0 },
            content_type: content_type.take().unwrap_or_default(),
            checksum: checksum
                .take()
                .map(|checksum| checksum.to_vec())
                .unwrap_or_default(),
            chunk: bytes[start..end].to_vec(),
            request_id: request_id.take().unwrap_or_default(),
        }
    })
}

/// File metadata attached only to the first `UploadRequest` produced by
/// [`rechunk_upload_requests`]; every later message leaves these fields at
/// their protobuf default (see [`chunk_upload_requests`] for why).
struct UploadMetadata {
    tenant_id: String,
    bucket: String,
    file_id: String,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
}

/// Mutable state driving [`rechunk_upload_requests`]'s `stream::unfold`,
/// boxed and type-erased over the caller's source stream so the state
/// itself stays a plain, non-generic type.
struct RechunkState {
    source: Pin<Box<dyn Stream<Item = std::io::Result<Bytes>> + Send>>,
    /// Bytes accumulated toward the next outgoing chunk. Never allowed to
    /// grow past [`DEFAULT_CHUNK_SIZE`]: every place that adds to it copies
    /// in at most the space remaining before that cap (see
    /// [`RechunkState::ingest`] and [`RechunkState::drain_pending`]), so a
    /// source that yields one huge item — a whole file handed over as a
    /// single [`Bytes`], say — still only ever grows this buffer one
    /// bounded slice at a time, never in a single copy that jumps straight
    /// to the item's full size.
    buffer: Vec<u8>,
    /// The unread tail of a source item that didn't fully fit into
    /// `buffer` when [`RechunkState::ingest`] received it, together with
    /// `pending_offset` marking how much of it has been copied into
    /// `buffer` so far. Drained into `buffer` in further bounded slices by
    /// [`RechunkState::drain_pending`] as room frees up, instead of ever
    /// being copied in all at once. Holding it as the [`Bytes`] the source
    /// yielded keeps this a refcount bump rather than a copy of the tail.
    pending: Bytes,
    /// How many bytes at the front of `pending` have already been copied
    /// into `buffer`. `pending` is reset to an empty [`Bytes`] once this
    /// reaches `pending.len()`, so a fully drained oversized item does not
    /// linger in memory (and, for a [`Bytes`] slice of a larger allocation,
    /// does not keep that allocation alive) waiting to be reused.
    pending_offset: usize,
    size_bytes: u64,
    total_written: u64,
    wrote_any: bool,
    source_exhausted: bool,
    metadata: Option<UploadMetadata>,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
}

impl RechunkState {
    /// Hand `error` to [`RociaDbClient::upload_file_chunked`] through
    /// `error_slot`, the only channel out of a stream whose item type is a
    /// bare `UploadRequest`. Written once and then read once, after the RPC
    /// settles; the outgoing stream ends immediately afterwards, so nothing
    /// overwrites it.
    fn record_error(&self, error: RociaDbError) {
        let mut guard = self
            .error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *guard = Some(error);
    }

    /// Declared bytes this state is holding but has not emitted yet: whatever
    /// is buffered toward the next chunk, plus the undrained tail of an
    /// oversized source item.
    ///
    /// `buffer` alone is not that figure — it is capped at
    /// [`DEFAULT_CHUNK_SIZE`], and anything beyond waits in `pending` — which
    /// is exactly why an overshoot has to be measured against both. A single
    /// source item carrying twice the declared size puts one chunk in `buffer`
    /// and the rest in `pending`, and the chunk on its own looks perfectly
    /// legal.
    fn unsent_len(&self) -> u64 {
        let pending_left = self.pending.len().saturating_sub(self.pending_offset);
        (self.buffer.len() + pending_left) as u64
    }

    /// Record the "would exceed / falls short of `size_bytes`" validation
    /// error into `error_slot`, so [`RociaDbClient::upload_file_chunked`]
    /// can surface it after the stream this state drives has ended.
    fn record_size_error(&self, message: String) {
        self.record_error(RociaDbError::validation(message));
    }

    /// Record a failed read from the caller's own source, which is neither the
    /// caller's arithmetic (a [`RociaDbError::Validation`]) nor anything the
    /// server said.
    fn record_io_error(&self, error: std::io::Error) {
        self.record_error(RociaDbError::Io {
            context: "the upload chunk stream",
            source: error,
        });
    }

    /// Turn `chunk` into the next `UploadRequest`, attaching the file
    /// metadata only if this is the first request ever produced (mirrors
    /// [`chunk_upload_requests`]'s `index == 0` special case).
    fn next_request(&mut self, chunk: Vec<u8>) -> UploadRequest {
        match self.metadata.take() {
            Some(metadata) => UploadRequest {
                tenant_id: metadata.tenant_id,
                bucket: metadata.bucket,
                file_id: metadata.file_id,
                size_bytes: self.size_bytes,
                content_type: metadata.content_type,
                checksum: metadata.checksum.to_vec(),
                chunk,
                request_id: metadata.request_id,
            },
            None => UploadRequest {
                chunk,
                ..Default::default()
            },
        }
    }

    /// Copy a freshly received source item into `buffer`, never in one
    /// piece larger than the space currently left before
    /// [`DEFAULT_CHUNK_SIZE`]. When `piece` is bigger than that space, only
    /// its head is copied in now; the tail becomes `pending` (with
    /// `pending_offset` marking that the head has already been accounted
    /// for) to be drained in further bounded slices by
    /// [`RechunkState::drain_pending`] on later polls, once `buffer` has
    /// been emptied out by an emitted chunk.
    ///
    /// This is the fix for the failure mode this module's docs warn about:
    /// without it, a single `Vec::extend` call with an oversized `piece`
    /// (for example a caller who already holds the whole file as one
    /// in-memory [`Bytes`] and yields it as a single stream item) would
    /// grow `buffer` straight past one output chunk's worth, buffering
    /// memory proportional to the whole file despite this function's docs
    /// promising otherwise.
    fn ingest(&mut self, piece: Bytes) {
        let space_left = DEFAULT_CHUNK_SIZE - self.buffer.len();
        if piece.len() <= space_left {
            self.buffer.extend_from_slice(&piece);
        } else {
            self.buffer.extend_from_slice(&piece[..space_left]);
            self.pending = piece;
            self.pending_offset = space_left;
        }
    }

    /// `true` while `pending` still holds bytes that have not yet been
    /// copied into `buffer`.
    fn has_pending(&self) -> bool {
        self.pending_offset < self.pending.len()
    }

    /// Copy as much of the unread tail of `pending` into `buffer` as fits
    /// in the space left before [`DEFAULT_CHUNK_SIZE`], advancing
    /// `pending_offset`, and free `pending` entirely once it has all been
    /// copied over. Called ahead of pulling the next item from `source`,
    /// so an oversized item already in `pending` finishes draining — in
    /// chunk-sized slices, interleaved with emitting the chunks `buffer`
    /// fills up to — before any more memory is pulled in from upstream.
    fn drain_pending(&mut self) {
        if !self.has_pending() {
            return;
        }
        let space_left = DEFAULT_CHUNK_SIZE - self.buffer.len();
        let available = self.pending.len() - self.pending_offset;
        let take = space_left.min(available);
        let end = self.pending_offset + take;
        self.buffer
            .extend_from_slice(&self.pending[self.pending_offset..end]);
        self.pending_offset = end;
        if !self.has_pending() {
            self.pending = Bytes::new();
            self.pending_offset = 0;
        }
    }
}

/// Re-chunk an arbitrarily-sized byte stream into `UploadRequest` messages
/// of exactly [`DEFAULT_CHUNK_SIZE`] (1 MiB) each — the last one possibly
/// shorter — the core of [`RociaDbClient::upload_file_chunked`]. Never
/// buffers more than one outgoing chunk's worth of bytes at a time, unlike
/// [`chunk_upload_requests`], which already holds the complete file in
/// memory by the time it runs. That bound holds regardless of how `chunks`
/// happens to be split: a single source item larger than one chunk — even
/// one as large as the whole file — is still copied into the outgoing
/// buffer through [`RechunkState::ingest`] and [`RechunkState::drain_pending`]
/// a bounded slice at a time rather than in one `extend` call, so it can
/// never grow the buffer past [`DEFAULT_CHUNK_SIZE`].
///
/// Validates as it goes: a chunk that would push the running total past
/// `size_bytes` is rejected *before* being turned into a request (so it is
/// never sent), and running short of `size_bytes` once `chunks` is
/// exhausted is detected right after the last real chunk. An `Err` item from
/// `chunks` ends everything at once — nothing further is pulled from the
/// source, no further request is emitted, and the [`std::io::Error`] is
/// reported as [`RociaDbError::Io`]. Because the returned
/// `Stream<Item = UploadRequest>` has no channel of its own to carry an error
/// — a tonic client-streaming call only accepts a stream that produces
/// requests, never `Result`s — any such failure is recorded into `error_slot`
/// instead, and the stream simply ends early (or, for a short source, ends
/// normally after reporting the mismatch). The caller (see
/// [`RociaDbClient::upload_file_chunked`]) checks `error_slot` once the RPC
/// settles.
///
/// An empty source (`size_bytes` 0, no bytes at all) still produces exactly
/// one empty request, because the server only learns the file's metadata
/// from a message, and an upload that writes nothing would never deliver
/// it — the same rule [`chunk_upload_requests`] applies for a zero-byte
/// in-memory buffer.
#[allow(clippy::too_many_arguments)]
fn rechunk_upload_requests<S>(
    tenant_id: String,
    bucket: String,
    file_id: String,
    size_bytes: u64,
    content_type: String,
    checksum: [u8; 32],
    request_id: String,
    chunks: S,
    error_slot: Arc<Mutex<Option<RociaDbError>>>,
) -> impl Stream<Item = UploadRequest>
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    let state = RechunkState {
        source: Box::pin(chunks),
        buffer: Vec::new(),
        pending: Bytes::new(),
        pending_offset: 0,
        size_bytes,
        total_written: 0,
        wrote_any: false,
        source_exhausted: false,
        metadata: Some(UploadMetadata {
            tenant_id,
            bucket,
            file_id,
            content_type,
            checksum,
            request_id,
        }),
        error_slot,
    };

    stream::unfold(state, |mut state| async move {
        loop {
            // The one overshoot check, against every declared byte this state
            // has seen — sent, buffered, or still pending — rather than against
            // the single chunk about to go out. Measured here, before anything
            // is emitted, because the alternative has a failure mode: a chunk
            // that is legal on its own can be the chunk that completes
            // `size_bytes`, and once it is on the wire the server has a valid
            // stream and commits it. Reporting a `Validation` error afterwards
            // then tells the caller the upload failed about a file that is
            // published under their `file_id` — truncated.
            if state.total_written + state.unsent_len() > state.size_bytes {
                state.record_size_error(format!(
                    "upload_file_chunked received more data than size_bytes \
                     ({} bytes) declared",
                    state.size_bytes
                ));
                return None;
            }

            // A full chunk goes out as soon as one is buffered — unless it is
            // the chunk that *completes* the declared total, which waits until
            // the source has confirmed it has nothing more. That confirmation
            // is the whole point: while more data could still arrive, emitting
            // the completing chunk is what would hand the server a stream it
            // considers whole.
            let completes_the_file =
                state.total_written + DEFAULT_CHUNK_SIZE as u64 == state.size_bytes;
            if state.buffer.len() >= DEFAULT_CHUNK_SIZE
                && (!completes_the_file || state.source_exhausted)
            {
                let piece: Vec<u8> = state.buffer.drain(..DEFAULT_CHUNK_SIZE).collect();
                state.total_written += DEFAULT_CHUNK_SIZE as u64;
                state.wrote_any = true;
                let request = state.next_request(piece);
                return Some((request, state));
            }

            // Drain any tail left over from an oversized source item before
            // pulling more data in, so it empties out in bounded slices —
            // interleaved with the chunk emissions above — rather than
            // sitting fully copied in `buffer` or growing it past
            // `DEFAULT_CHUNK_SIZE` on some later `ingest` call.
            if state.has_pending() {
                state.drain_pending();
                continue;
            }

            if !state.source_exhausted {
                match state.source.next().await {
                    Some(Ok(piece)) => {
                        state.ingest(piece);
                        continue;
                    }
                    Some(Err(error)) => {
                        // The caller's source failed. Stop pulling from it —
                        // returning `None` here drops the state, and with it
                        // the source — and end the outgoing stream, whatever
                        // is still buffered.
                        //
                        // Whether that failure is the caller's answer depends
                        // on whether every declared byte is already in hand —
                        // sent *or* buffered. Short of `size_bytes`, sending
                        // what was read would only produce a truncated upload
                        // the server would reject for a reason unrelated to
                        // what went wrong, so the recorded error wins over that
                        // rejection in `upload_file_chunked`.
                        //
                        // With all of them in hand, nothing is missing: the
                        // source failed after the last byte this upload needed,
                        // so finish it and let the server judge. Recording an
                        // error instead would report a failure for a file that
                        // ends up stored and correct, and a caller reading that
                        // as "nothing was written" deletes or re-queues it. The
                        // source is deliberately read one item past the
                        // declared total, since that is how an overshoot is
                        // caught, so ending with an error right after the last
                        // byte is the ordinary shape of this rather than a
                        // corner case.
                        //
                        // Measured against `unsent_len` rather than
                        // `total_written` alone so it holds at *any*
                        // `size_bytes`: a file smaller than one chunk has its
                        // whole content buffered and nothing emitted yet, and
                        // an earlier version of this check compared only what
                        // had been sent — so the same source failure was
                        // forgiven at an exact 1 MiB multiple and reported at
                        // every other size, which is not a distinction a caller
                        // could have predicted.
                        //
                        // `size_bytes > 0` is the one carve-out, and it is
                        // about consequences rather than symmetry. A zero-byte
                        // declaration has every byte it declared in hand before
                        // the source is read at all, so the rule above would
                        // forgive a source that failed on its very first poll
                        // and publish an empty file. Publishing is a
                        // *replacement*: the server swaps the published version
                        // for this `file_id` in one step, so a caller whose size
                        // computation returned 0 by mistake — an empty read, a
                        // stat that raced a writer — and whose source then
                        // failed would silently destroy the file already stored
                        // there and be told `Ok`. With no bytes to read, a read
                        // failure says nothing about the file's contents, but it
                        // does say the caller's source is broken; reporting that
                        // costs a spurious error, and not reporting it costs
                        // data.
                        if state.size_bytes > 0
                            && state.total_written + state.unsent_len() == state.size_bytes
                        {
                            warn!(
                                error = %error,
                                size_bytes = state.size_bytes,
                                "the upload chunk stream failed after every declared byte had \
                                 been read; completing the upload and reporting the server's \
                                 verdict instead"
                            );
                            // Nothing more will be pulled: the source is done,
                            // however it ended. Fall through and flush.
                            state.source_exhausted = true;
                            continue;
                        }
                        state.record_io_error(error);
                        return None;
                    }
                    None => {
                        state.source_exhausted = true;
                        continue;
                    }
                }
            }

            // Source exhausted, less than one full chunk buffered: flush
            // the remainder (possibly empty, for a zero-byte file).
            if !state.buffer.is_empty() || !state.wrote_any {
                // No overshoot check here: the one at the top of the loop has
                // already compared `total_written + unsent_len()` against
                // `size_bytes`, and what is left in `buffer` is a subset of
                // `unsent_len()`, so reaching this point means it fits.
                let piece_len = state.buffer.len() as u64;
                state.total_written += piece_len;
                state.wrote_any = true;
                let piece = std::mem::take(&mut state.buffer);
                let request = state.next_request(piece);
                return Some((request, state));
            }

            if state.total_written != state.size_bytes {
                state.record_size_error(format!(
                    "upload_file_chunked sent {} bytes but size_bytes declared {}",
                    state.total_written, state.size_bytes
                ));
            }
            return None;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{
        AsyncWrite, AsyncWriteExt, DEFAULT_CHUNK_SIZE, DEFAULT_MAX_FILE_BYTES,
        DOWNLOAD_WRITE_CONTEXT, FILE_TIMESTAMP_CONTEXT, FileMetadata, FileStreamUploadOptions,
        FileTimestamp, FileUploadOptions, INLINE_CHECKSUM_LIMIT, MAX_PREALLOCATED_DOWNLOAD_BYTES,
        MAX_QUOTED_TIMESTAMP_CHARS, RechunkState, VerifiedDownload, chunk_upload_requests,
        days_from_civil, default_upload_file_request_id, is_leap_year, parse_rfc3339,
        preallocated_download_capacity, rechunk_upload_requests, resolve_checksum,
        resolve_checksum_offloaded, validate_file_size,
    };
    use crate::pb::upstream::v1::{StatResponse, UploadRequest};
    use crate::test_support::{lazy_test_client, lazy_test_client_with_max_file_bytes};
    use crate::{Bytes, RociaDbError};
    use futures::executor::block_on;
    use futures::{StreamExt, stream};
    use sha2::{Digest, Sha256};
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::{Duration, UNIX_EPOCH};

    /// Length of a SHA-256 digest. The production code no longer needs this
    /// as a constant — `[u8; 32]` carries it — but the tests still assert
    /// against the number itself.
    const CHECKSUM_LEN: usize = 32;

    /// An [`AsyncWrite`] that takes `allowed` bytes and then fails every
    /// further write, standing in for the destination of a download running out
    /// of room — a full disk, a closed socket — partway through.
    ///
    /// Never returns `Ok(0)`: that is how an `AsyncWrite` reports "no progress
    /// but not an error", and `write_all` turns it into a `WriteZero` of its own
    /// rather than the failure this stands for.
    struct FailingWriter {
        written: usize,
        allowed: usize,
    }

    impl FailingWriter {
        fn new(allowed: usize) -> Self {
            Self {
                written: 0,
                allowed,
            }
        }
    }

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let room = self.allowed.saturating_sub(self.written);
            if room == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "the download destination went away",
                )));
            }
            let accepted = room.min(buf.len());
            self.written += accepted;
            Poll::Ready(Ok(accepted))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// One source item, as `upload_file_chunked`'s stream yields them.
    fn chunk(bytes: Vec<u8>) -> std::io::Result<Bytes> {
        Ok(Bytes::from(bytes))
    }

    /// A failing source item, standing in for a read that died partway
    /// through the caller's file or socket.
    fn read_failure() -> std::io::Result<Bytes> {
        Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "the source stopped reading",
        ))
    }

    #[test]
    fn upload_options_have_safe_defaults() {
        let options = FileUploadOptions::new();
        assert_eq!(options, FileUploadOptions::default());
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.checksum.is_none());
        assert!(options.request_id.is_none());
    }

    #[test]
    fn upload_options_setters_are_chainable_and_readable() {
        let options = FileUploadOptions::new()
            .with_content_type("text/csv")
            .with_checksum([7u8; CHECKSUM_LEN])
            .with_request_id("retry-1");
        assert_eq!(options.content_type, "text/csv");
        assert_eq!(options.checksum, Some([7u8; CHECKSUM_LEN]));
        assert_eq!(options.request_id.as_deref(), Some("retry-1"));
    }

    #[test]
    fn file_stream_upload_options_require_size_and_checksum_and_default_the_rest() {
        let options = FileStreamUploadOptions::new(1234, [3u8; CHECKSUM_LEN]);
        assert_eq!(options.size_bytes, 1234);
        assert_eq!(options.checksum, [3u8; CHECKSUM_LEN]);
        assert_eq!(options.content_type, "application/octet-stream");
        assert!(options.request_id.is_none());

        let options = options
            .with_content_type("application/pdf")
            .with_request_id("retry-2");
        assert_eq!(options.content_type, "application/pdf");
        assert_eq!(options.request_id.as_deref(), Some("retry-2"));
        // The required fields survive the chained setters.
        assert_eq!(options.size_bytes, 1234);
        assert_eq!(options.checksum, [3u8; CHECKSUM_LEN]);
    }

    #[test]
    fn default_upload_request_id_uses_the_upload_file_prefix_with_a_fresh_uuid_each_time() {
        let first = default_upload_file_request_id();
        let second = default_upload_file_request_id();
        let uuid_part = first
            .strip_prefix("upload_file:")
            .expect("default request_id must use the upload_file: prefix");
        uuid::Uuid::parse_str(uuid_part).expect("suffix after the prefix must be a uuid");
        assert_ne!(first, second, "each call must mint a fresh idempotency key");
    }

    #[test]
    fn upload_requests_chunk_at_exactly_one_mebibyte() {
        let bytes = vec![7u8; DEFAULT_CHUNK_SIZE + 10];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(bytes.clone()),
            "text/plain".into(),
            [0u8; CHECKSUM_LEN],
            "stable-request".into(),
        )
        .collect();

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 10);
        assert_eq!(requests[0].size_bytes, bytes.len() as u64);
        // Only the first message carries metadata; the server ignores the
        // rest of the fields on later messages.
        assert_eq!(requests[1].size_bytes, 0);
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].checksum.len(), CHECKSUM_LEN);
        assert!(requests[1].checksum.is_empty());
        assert!(
            requests
                .iter()
                .all(|request| request.request_id == "stable-request"
                    || request.request_id.is_empty())
        );
        assert_eq!(requests[0].request_id, "stable-request");
    }

    #[test]
    fn empty_upload_still_emits_one_request() {
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(Vec::new()),
            FileUploadOptions::default().content_type,
            [0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].size_bytes, 0);
        assert!(requests[0].chunk.is_empty());
    }

    /// Asserts that chunking `total_bytes` matches the wire contract
    /// described in [`crate::RociaDbClient::upload_file_stream`]: every
    /// chunk but the last is exactly 1 MiB, the last is non-empty and no
    /// larger than 1 MiB, and the sum of chunk bytes equals `size_bytes`.
    fn assert_chunking_matches_server_contract(total_bytes: usize) {
        let bytes = vec![9u8; total_bytes];
        let requests: Vec<_> = chunk_upload_requests(
            "tenant".into(),
            "bucket".into(),
            "file".into(),
            Arc::new(bytes.clone()),
            "application/octet-stream".into(),
            [0u8; CHECKSUM_LEN],
            "req".into(),
        )
        .collect();

        assert!(!requests.is_empty(), "at least one message is required");
        assert_eq!(requests[0].size_bytes, total_bytes as u64);

        let bytes_sent: usize = requests.iter().map(|request| request.chunk.len()).sum();
        assert_eq!(
            bytes_sent, total_bytes,
            "sum of chunk bytes must equal size_bytes exactly"
        );

        if total_bytes == 0 {
            assert_eq!(requests.len(), 1);
            assert!(requests[0].chunk.is_empty());
            return;
        }

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty(), "the last chunk must not be empty");
        assert!(
            last.chunk.len() <= DEFAULT_CHUNK_SIZE,
            "the last chunk must not exceed 1 MiB"
        );
    }

    #[test]
    fn chunking_zero_bytes() {
        assert_chunking_matches_server_contract(0);
    }

    #[test]
    fn chunking_one_byte() {
        assert_chunking_matches_server_contract(1);
    }

    #[test]
    fn chunking_exactly_one_mebibyte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn chunking_one_mebibyte_plus_one_byte() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE + 1);
    }

    #[test]
    fn chunking_about_two_and_a_half_mebibytes() {
        assert_chunking_matches_server_contract(DEFAULT_CHUNK_SIZE * 2 + DEFAULT_CHUNK_SIZE / 2);
    }

    #[test]
    fn resolve_checksum_computes_sha256_by_default() {
        // Known-answer test for SHA-256("hello world"), independent of the
        // crate's own `Sha256::digest` call, so a wiring mistake (wrong
        // input bytes, wrong algorithm) would be caught even if it still
        // happened to produce 32 bytes.
        let checksum = resolve_checksum(None, b"hello world");
        assert_eq!(
            checksum.to_vec(),
            decode_hex("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
        );
    }

    #[test]
    fn resolve_checksum_is_deterministic_and_content_dependent() {
        let first = resolve_checksum(None, b"payload-a");
        let second = resolve_checksum(None, b"payload-a");
        let different = resolve_checksum(None, b"payload-b");
        assert_eq!(first, second, "same bytes must yield the same checksum");
        assert_ne!(
            first, different,
            "different bytes must yield a different checksum"
        );
    }

    #[tokio::test]
    async fn resolve_checksum_offloaded_agrees_with_the_inline_digest() {
        // Either side of the threshold, so both branches are covered, and the
        // caller-supplied case that must skip hashing entirely.
        for len in [0usize, 1, INLINE_CHECKSUM_LIMIT, INLINE_CHECKSUM_LIMIT + 1] {
            let bytes: Arc<Vec<u8>> = Arc::new((0..len).map(|index| index as u8).collect());
            assert_eq!(
                resolve_checksum_offloaded(None, &bytes).await,
                resolve_checksum(None, &bytes),
                "offloading must not change the digest, len={len}"
            );
        }

        let supplied = [3u8; CHECKSUM_LEN];
        let bytes: Arc<Vec<u8>> = Arc::new(vec![9u8; INLINE_CHECKSUM_LIMIT * 2]);
        assert_eq!(
            resolve_checksum_offloaded(Some(supplied), &bytes).await,
            supplied,
            "a caller-supplied digest must never be recomputed, offloaded or not"
        );
    }

    #[test]
    fn resolve_checksum_accepts_a_caller_supplied_digest_verbatim() {
        let supplied = [7u8; CHECKSUM_LEN];
        let checksum = resolve_checksum(Some(supplied), b"irrelevant");
        assert_eq!(
            checksum, supplied,
            "a caller-supplied digest must be sent as-is, never recomputed"
        );
    }

    fn decode_hex(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex pair"))
            .collect()
    }

    #[test]
    fn the_verified_download_preallocation_is_capped_well_below_the_file_size_limit() {
        // The point of the cap: `download_file_verified` reserves
        // `min(size_bytes, cap)` up front, so the biggest allocation a
        // server can provoke with a made-up `size_bytes` is the cap — not
        // the 5 GiB a file is allowed to reach by default, and not the 2^64
        // an unchecked `u64` would allow.
        assert_eq!(MAX_PREALLOCATED_DOWNLOAD_BYTES, 64 * 1024 * 1024);
        const {
            assert!(MAX_PREALLOCATED_DOWNLOAD_BYTES < DEFAULT_MAX_FILE_BYTES);
        }
        for reported in [0, 1, 4096, MAX_PREALLOCATED_DOWNLOAD_BYTES, u64::MAX] {
            let capacity = preallocated_download_capacity(reported);
            assert!(
                capacity as u64 <= MAX_PREALLOCATED_DOWNLOAD_BYTES,
                "a reported size of {reported} must never reserve more than the cap"
            );
        }
        // Below the cap the reported size is reserved exactly, so a realistic
        // file is allocated once rather than grown chunk by chunk.
        assert_eq!(preallocated_download_capacity(4096), 4096);
        assert_eq!(
            preallocated_download_capacity(u64::MAX),
            usize::try_from(MAX_PREALLOCATED_DOWNLOAD_BYTES).expect("64 MiB fits in a usize")
        );
    }

    /// Drive a [`VerifiedDownload`] over `chunks` into `writer` exactly as
    /// `RociaDbClient::download_verified_into` drives it over a real download
    /// stream: record and write each chunk, check the totals, then flush.
    ///
    /// No server, no stream, and no tokio runtime — neither a `Vec<u8>` nor
    /// [`FailingWriter`] ever reaches the reactor, so
    /// `futures::executor::block_on` is enough to drive the writes.
    fn verify_download<W>(
        expected_size: u64,
        expected_checksum: &[u8],
        chunks: &[&[u8]],
        writer: &mut W,
    ) -> crate::Result<u64>
    where
        W: AsyncWrite + Unpin + ?Sized,
    {
        let mut verified = VerifiedDownload::new(expected_size);
        for chunk in chunks {
            block_on(verified.absorb(writer, chunk))?;
        }
        let written = verified.finish(expected_checksum)?;
        block_on(writer.flush()).map_err(super::download_write_error)?;
        Ok(written)
    }

    #[test]
    fn verified_download_checks_a_stream_against_its_metadata_with_no_writer_at_all() {
        // `record` and `finish` are pure: they are the whole of the rule both
        // verified downloads apply, and neither needs a destination to apply
        // it. Everything below exercises them through `absorb`; this pins down
        // that they stand on their own.
        let payload = b"the quick brown fox";
        let mut verified = VerifiedDownload::new(payload.len() as u64);
        for chunk in payload.chunks(4) {
            verified
                .record(chunk)
                .expect("no chunk overshoots the size");
        }
        let digest: [u8; 32] = Sha256::digest(payload).into();
        assert_eq!(
            verified
                .finish(&digest)
                .expect("a matching size and digest must verify"),
            payload.len() as u64
        );
    }

    #[test]
    fn verified_download_writes_every_chunk_and_reports_the_byte_count() {
        let payload: Vec<u8> = (0..5000u32).map(|byte| (byte % 251) as u8).collect();
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let chunks: Vec<&[u8]> = payload.chunks(997).collect();
        let mut written = Vec::new();
        let count = verify_download(payload.len() as u64, &digest, &chunks, &mut written)
            .expect("a stream that matches its metadata must verify");
        assert_eq!(count, payload.len() as u64);
        assert_eq!(
            written, payload,
            "the writer must receive the file byte for byte, in order"
        );
    }

    #[test]
    fn writing_through_async_write_keeps_the_buffers_preallocated_capacity() {
        // The property that lets `download_file_verified` be the writer variant
        // pointed at a `Vec<u8>` without losing its capped pre-allocation:
        // `<Vec<u8> as AsyncWrite>::poll_write` is an `extend_from_slice`, so a
        // buffer that was reserved up front is filled rather than regrown.
        let payload: Vec<u8> = vec![1u8; 4096];
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let chunks: Vec<&[u8]> = payload.chunks(101).collect();
        let capacity = preallocated_download_capacity(payload.len() as u64);
        let mut written = Vec::with_capacity(capacity);
        verify_download(payload.len() as u64, &digest, &chunks, &mut written)
            .expect("the download must verify");
        assert_eq!(
            written.capacity(),
            capacity,
            "filling a pre-allocated buffer through AsyncWrite must not reallocate it"
        );
    }

    #[test]
    fn verified_download_rejects_a_digest_the_bytes_do_not_satisfy() {
        let payload = b"stored bytes".as_slice();
        let mut written = Vec::new();
        let error = verify_download(
            payload.len() as u64,
            &[9u8; CHECKSUM_LEN],
            &[payload],
            &mut written,
        )
        .expect_err("a digest that disagrees must fail verification");
        match error {
            RociaDbError::ChecksumMismatch { expected, actual } => {
                assert_eq!(expected, vec![9u8; CHECKSUM_LEN]);
                assert_eq!(actual, Sha256::digest(payload).to_vec());
            }
            other => panic!("expected a checksum mismatch, got: {other}"),
        }
        assert_eq!(
            written, payload,
            "the writer has already received the whole file: discarding it is the caller's job"
        );
    }

    #[test]
    fn verified_download_rejects_a_stream_shorter_than_its_metadata() {
        let payload = b"four".as_slice();
        let mut written = Vec::new();
        let error = verify_download(64, &[0u8; CHECKSUM_LEN], &[payload], &mut written)
            .expect_err("a short stream must fail");
        assert!(
            matches!(
                error,
                RociaDbError::SizeMismatch {
                    expected: 64,
                    actual: 4
                }
            ),
            "the size is checked before the digest, got: {error}"
        );
    }

    #[test]
    fn verified_download_stops_at_the_chunk_that_overshoots_without_writing_it() {
        // The early exit: the chunk that pushes the total past `size_bytes` is
        // recorded (so `actual` counts it, exactly as the buffered variant
        // always reported) but never handed to the writer, so a server that
        // never stops sending cannot fill the caller's disk.
        let mut written = Vec::new();
        let error = verify_download(
            6,
            &[0u8; CHECKSUM_LEN],
            &[b"abcd".as_slice(), b"efgh".as_slice(), b"ijkl".as_slice()],
            &mut written,
        )
        .expect_err("more bytes than the metadata declared must fail");
        assert!(
            matches!(
                error,
                RociaDbError::SizeMismatch {
                    expected: 6,
                    actual: 8
                }
            ),
            "got: {error}"
        );
        assert_eq!(
            written, b"abcd",
            "only the chunks that fitted may have been written, and nothing after the overshoot"
        );
    }

    #[test]
    fn verified_download_surfaces_a_writer_that_fails_after_n_bytes_as_an_io_error() {
        let payload: Vec<u8> = vec![3u8; 300];
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let chunks: Vec<&[u8]> = payload.chunks(100).collect();
        // Room for the first chunk and half of the second.
        let mut writer = FailingWriter::new(150);
        let error = verify_download(payload.len() as u64, &digest, &chunks, &mut writer)
            .expect_err("a writer that fails must fail the download");
        let RociaDbError::Io { context, source } = &error else {
            panic!("a failing writer must produce RociaDbError::Io, got: {error}");
        };
        assert_eq!(*context, DOWNLOAD_WRITE_CONTEXT);
        assert_eq!(*context, "writing the downloaded file");
        assert_eq!(source.kind(), std::io::ErrorKind::BrokenPipe);
        assert!(
            error
                .to_string()
                .contains("the download destination went away"),
            "Display must fold in the writer's own message, got: {error}"
        );
        assert!(
            error.code().is_none(),
            "a failing writer is not a gRPC status, got: {error}"
        );
        assert_eq!(
            writer.written, 150,
            "everything the writer did accept must have been written before it refused"
        );
    }

    #[test]
    fn the_default_file_size_limit_mirrors_the_servers_five_gibibyte_default() {
        assert_eq!(DEFAULT_MAX_FILE_BYTES, 5 * 1024 * 1024 * 1024);
    }

    #[test]
    fn validate_file_size_accepts_exactly_the_limit() {
        validate_file_size(DEFAULT_MAX_FILE_BYTES, DEFAULT_MAX_FILE_BYTES)
            .expect("exactly the limit must be accepted");
    }

    #[test]
    fn validate_file_size_rejects_one_byte_over_the_limit_and_names_both_counts() {
        let error = validate_file_size(DEFAULT_MAX_FILE_BYTES + 1, DEFAULT_MAX_FILE_BYTES)
            .expect_err("one byte over the limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(
            message.contains(&(DEFAULT_MAX_FILE_BYTES + 1).to_string()),
            "the file's own size must be readable, got: {message}"
        );
        assert!(
            message.contains(&DEFAULT_MAX_FILE_BYTES.to_string()),
            "the limit must be readable, got: {message}"
        );
        assert!(
            message.contains("max_file_bytes"),
            "the message must name the setter to change, got: {message}"
        );
    }

    #[test]
    fn validate_file_size_follows_the_limit_it_is_given_in_both_directions() {
        // The limit is a parameter precisely so
        // `RociaDbBuilder::max_file_bytes` can move it: a file the 5 GiB
        // default waves through must be rejected under a lowered one, and a
        // raised one must accept a file no server default would.
        let one_mib = DEFAULT_CHUNK_SIZE as u64;
        validate_file_size(one_mib, DEFAULT_MAX_FILE_BYTES)
            .expect("1 MiB is far under the default limit");
        let error = validate_file_size(one_mib, 1024)
            .expect_err("a lowered limit must reject what the default accepts");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("1024"));
        validate_file_size(DEFAULT_MAX_FILE_BYTES * 2, DEFAULT_MAX_FILE_BYTES * 4)
            .expect("a raised limit must accept a file over the default");
    }

    // `upload_file_chunked`'s pre-flight size validation must run — and
    // fail — before the method ever touches the network, so this runs
    // against a client wired to an unreachable host and must still return
    // promptly. (The checksum length is no longer validated at runtime:
    // `[u8; 32]` makes a wrong length unrepresentable.)
    #[tokio::test]
    async fn upload_file_chunked_rejects_an_oversized_file_before_any_network_call() {
        let client = lazy_test_client();
        let oversized = DEFAULT_MAX_FILE_BYTES + 1;
        let error = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "file",
                stream::empty::<std::io::Result<Bytes>>(),
                FileStreamUploadOptions::new(oversized, [0u8; CHECKSUM_LEN]),
            )
            .await
            .expect_err("a file over the default 5 GiB limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains(&oversized.to_string()));
    }

    // Both ergonomic uploads read the ceiling off the *client*, not off the
    // constant, so a client built with a lowered `max_file_bytes` rejects a
    // file the default would have sent — and still does so before touching
    // the network, which is why an unreachable host does not make this hang.
    #[tokio::test]
    async fn both_ergonomic_uploads_honour_the_clients_own_max_file_bytes() {
        let client = lazy_test_client_with_max_file_bytes(1024);

        let error = client
            .upload_file(
                "tenant",
                "bucket",
                "buffered.bin",
                vec![7u8; 1025],
                FileUploadOptions::new(),
            )
            .await
            .expect_err("a file over the client's own limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("1024"));

        let error = client
            .upload_file_chunked(
                "tenant",
                "bucket",
                "streamed.bin",
                stream::empty::<std::io::Result<Bytes>>(),
                FileStreamUploadOptions::new(1025, [0u8; CHECKSUM_LEN]),
            )
            .await
            .expect_err("a declared size over the client's own limit must be rejected");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("1024"));

        // And exactly the limit still goes out: the gate is `>`, not `>=`.
        // (It fails at the connection stage against the unreachable host,
        // which is what proves the size check let it through.)
        let error = client
            .upload_file(
                "tenant",
                "bucket",
                "exactly.bin",
                vec![7u8; 1024],
                FileUploadOptions::new(),
            )
            .await
            .expect_err("the unreachable host must fail the call itself");
        assert!(
            !matches!(error, RociaDbError::Validation(_)),
            "a file of exactly the limit must pass the size gate, got: {error}"
        );
    }

    /// Drives [`rechunk_upload_requests`] to completion against an
    /// in-memory source and returns the produced requests, whatever error
    /// ended up in `error_slot`, and how many items were actually pulled from
    /// the source — the last of which is what proves nothing is pulled after
    /// an `Err`. No network, no tokio runtime needed: `stream::iter` resolves
    /// synchronously, so `futures::executor::block_on` alone is enough to
    /// drive the `stream::unfold` chain to its end.
    fn collect_rechunked_source(
        size_bytes: u64,
        source_items: Vec<std::io::Result<Bytes>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>, usize) {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let pulled = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&pulled);
        let source = stream::iter(source_items).inspect(move |_| {
            counter.fetch_add(1, Ordering::Relaxed);
        });
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                size_bytes,
                "application/octet-stream".into(),
                [0u8; CHECKSUM_LEN],
                "req".into(),
                source,
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        let error = error_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        (requests, error, pulled.load(Ordering::Relaxed))
    }

    /// [`collect_rechunked_source`] for a source that never fails, which is
    /// every test but the three about a failing one.
    fn collect_rechunked(
        size_bytes: u64,
        source_pieces: Vec<Vec<u8>>,
    ) -> (Vec<UploadRequest>, Option<RociaDbError>) {
        let (requests, error, _pulled) =
            collect_rechunked_source(size_bytes, source_pieces.into_iter().map(chunk).collect());
        (requests, error)
    }

    #[test]
    fn rechunk_exact_multiple_of_one_mebibyte_has_no_trailing_empty_chunk() {
        let total = DEFAULT_CHUNK_SIZE * 2;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![5u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(
            requests.len(),
            2,
            "an exact multiple of the chunk size must not emit a trailing empty request"
        );
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), DEFAULT_CHUNK_SIZE);
        // Only the first message carries metadata, exactly like
        // `chunk_upload_requests`.
        assert_eq!(requests[0].tenant_id, "tenant");
        assert!(requests[1].tenant_id.is_empty());
        assert_eq!(requests[0].size_bytes, total as u64);
        assert_eq!(requests[1].size_bytes, 0);
        assert_eq!(requests[0].checksum.len(), CHECKSUM_LEN);
        assert!(requests[1].checksum.is_empty());
    }

    #[test]
    fn rechunk_non_multiple_ends_with_a_short_last_chunk() {
        let total = DEFAULT_CHUNK_SIZE + 100;
        let (requests, error) = collect_rechunked(total as u64, vec![vec![9u8; total]]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].chunk.len(), DEFAULT_CHUNK_SIZE);
        assert_eq!(requests[1].chunk.len(), 100);
    }

    #[test]
    fn rechunk_reassembles_many_small_source_pieces_byte_for_byte() {
        // Feed the re-chunker a source split into many small (64 KiB)
        // pieces — nothing like the 1 MiB output chunk size — with a
        // distinctive byte pattern so any misordering or off-by-one
        // slicing bug would be caught, not just the total byte count.
        let piece_len = 64 * 1024;
        let piece_count = 40; // ~2.5 MiB total: spans multiple 1 MiB output chunks
        let mut expected = Vec::new();
        let mut pieces = Vec::new();
        for i in 0..piece_count {
            let piece: Vec<u8> = (0..piece_len).map(|b| ((i * 7 + b) % 256) as u8).collect();
            expected.extend_from_slice(&piece);
            pieces.push(piece);
        }
        let total = expected.len() as u64;
        let (requests, error) = collect_rechunked(total, pieces);
        assert!(error.is_none(), "unexpected validation error: {error:?}");

        let reassembled: Vec<u8> = requests.iter().flat_map(|r| r.chunk.clone()).collect();
        assert_eq!(
            reassembled, expected,
            "reassembled bytes must exactly match the source, regardless of how it was chunked \
             on input"
        );

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must be exactly 1 MiB"
            );
        }
        assert!(!last.chunk.is_empty());
        assert!(last.chunk.len() <= DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_zero_byte_file_still_emits_one_metadata_carrying_request() {
        let (requests, error) = collect_rechunked(0, vec![]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].chunk.is_empty());
        assert_eq!(requests[0].size_bytes, 0);
        assert_eq!(
            requests[0].tenant_id, "tenant",
            "the sole request of an empty upload must still carry file metadata, otherwise the \
             server never learns about the file"
        );
    }

    #[test]
    fn rechunk_rejects_more_data_than_declared_size_bytes_before_sending_the_offending_chunk() {
        let declared = DEFAULT_CHUNK_SIZE as u64; // caller declares only 1 MiB
        // the source produces 2 MiB in a single piece
        let (requests, error) =
            collect_rechunked(declared, vec![vec![1u8; DEFAULT_CHUNK_SIZE * 2]]);
        let error = error.expect("an overflow must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        assert!(error.to_string().contains("more data than size_bytes"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert!(
            sent <= declared as usize,
            "the chunk that would push the total past size_bytes must never be sent, got \
             {sent} bytes sent for a {declared}-byte declared size"
        );
    }

    #[test]
    fn rechunk_reports_a_shortfall_once_the_source_is_exhausted() {
        let declared = (DEFAULT_CHUNK_SIZE * 2) as u64; // caller declares 2 MiB
        // the source only ever produces 1 MiB
        let (requests, error) = collect_rechunked(declared, vec![vec![3u8; DEFAULT_CHUNK_SIZE]]);
        let error = error.expect("a shortfall must be recorded as a validation error");
        assert!(matches!(error, RociaDbError::Validation(_)));
        let message = error.to_string();
        assert!(message.contains("sent"));
        assert!(message.contains("but size_bytes declared"));
        let sent: usize = requests.iter().map(|r| r.chunk.len()).sum();
        assert_eq!(sent, DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn rechunk_honors_caller_supplied_request_id_and_content_type_on_the_first_request_only() {
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let requests: Vec<UploadRequest> = block_on(
            rechunk_upload_requests(
                "tenant".into(),
                "bucket".into(),
                "file".into(),
                10,
                "text/csv".into(),
                [0u8; CHECKSUM_LEN],
                "caller-request-id".into(),
                stream::iter(vec![chunk(vec![1u8; 10])]),
                Arc::clone(&error_slot),
            )
            .collect::<Vec<_>>(),
        );
        assert!(
            error_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none()
        );
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].content_type, "text/csv");
        assert_eq!(requests[0].request_id, "caller-request-id");
    }

    /// Asserts that `error` is the [`RociaDbError::Io`] the three failing-source
    /// tests expect, carrying the source's own `io::Error` rather than a
    /// message about byte counts.
    fn assert_is_the_source_read_failure(error: Option<RociaDbError>) {
        let error = error.expect("a failing source must be recorded as an error");
        let RociaDbError::Io { context, source } = &error else {
            panic!("a failing source must produce RociaDbError::Io, got: {error}");
        };
        assert_eq!(*context, "the upload chunk stream");
        assert_eq!(source.kind(), std::io::ErrorKind::UnexpectedEof);
        let message = error.to_string();
        assert!(
            message.contains("the source stopped reading"),
            "Display must fold in the source's own message, got: {message}"
        );
        assert!(
            !message.contains("size_bytes"),
            "a read failure must not be reported as a size mismatch, got: {message}"
        );
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_on_the_very_first_item() {
        // Zero items read before the failure: the upload must fail with the
        // I/O error and send nothing at all.
        let (requests, error, pulled) =
            collect_rechunked_source(4096, vec![read_failure(), chunk(vec![1u8; 4096])]);
        assert_is_the_source_read_failure(error);
        assert!(
            requests.is_empty(),
            "nothing may be sent when the source fails before producing a byte, got {} requests",
            requests.len()
        );
        assert_eq!(
            pulled, 1,
            "the item after the failure must never be pulled from the source"
        );
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_after_one_item() {
        // One short item, then a failure: the buffered bytes are dropped
        // rather than sent as a truncated file, and the error is the read's.
        let (requests, error, pulled) = collect_rechunked_source(
            (DEFAULT_CHUNK_SIZE * 2) as u64,
            vec![chunk(vec![7u8; 1024]), read_failure(), chunk(vec![9u8; 16])],
        );
        assert_is_the_source_read_failure(error);
        assert!(
            requests.is_empty(),
            "a partial first chunk must not be emitted once the source has failed"
        );
        assert_eq!(pulled, 2, "nothing after the failing item may be pulled");
    }

    #[test]
    fn rechunk_surfaces_a_source_failure_after_several_items_and_emitted_chunks() {
        // Enough bytes to have emitted two full 1 MiB messages before the
        // failure: the messages already sent stand (they are on the wire), the
        // upload still fails, and the error still names the read rather than
        // the truncated total.
        let (requests, error, pulled) = collect_rechunked_source(
            (DEFAULT_CHUNK_SIZE * 4) as u64,
            vec![
                chunk(vec![1u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![2u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![3u8; 512]),
                read_failure(),
                chunk(vec![4u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![5u8; DEFAULT_CHUNK_SIZE]),
            ],
        );
        assert_is_the_source_read_failure(error);
        assert_eq!(
            requests.len(),
            2,
            "only the chunks that were complete before the failure may have been sent"
        );
        assert!(
            requests
                .iter()
                .all(|request| request.chunk.len() == DEFAULT_CHUNK_SIZE)
        );
        assert_eq!(
            pulled, 4,
            "the source must be polled exactly up to and including the failing item"
        );
    }

    #[test]
    fn rechunk_ignores_a_source_failure_that_lands_after_the_declared_total() {
        // Every declared byte has already gone out as a full message, so the
        // server has a complete stream and will commit it. Recording the read
        // failure here would report an error for a file that is stored and
        // valid.
        let total = DEFAULT_CHUNK_SIZE * 2;
        let (requests, error, pulled) = collect_rechunked_source(
            total as u64,
            vec![
                chunk(vec![1u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![2u8; DEFAULT_CHUNK_SIZE]),
                read_failure(),
            ],
        );
        assert!(
            error.is_none(),
            "a failure after the last declared byte must not be recorded, got: {error:?}"
        );
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests
                .iter()
                .map(|request| request.chunk.len())
                .sum::<usize>(),
            total,
            "every declared byte must still have been sent"
        );
        assert_eq!(
            pulled, 3,
            "the overshoot check reads one item past the declared total, which is exactly \
             how this failure is reached"
        );
    }

    #[test]
    fn rechunk_sends_nothing_when_the_overshoot_is_a_whole_extra_chunk() {
        // The regression this pair of tests exists for. A `size_bytes` that is
        // an exact multiple of the chunk size used to be the one case where the
        // overshoot was noticed too late: the chunk completing the declared
        // total was legal on its own, went out, gave the server a whole stream
        // to commit — and only the *next* iteration saw the excess. The caller
        // got a `Validation` error for a truncated file published under their
        // own `file_id`.
        let (requests, error, _pulled) = collect_rechunked_source(
            DEFAULT_CHUNK_SIZE as u64,
            vec![chunk(vec![1u8; DEFAULT_CHUNK_SIZE * 2])],
        );
        assert!(
            requests.is_empty(),
            "not one byte may go out when the data in hand already exceeds size_bytes, got {} \
             requests",
            requests.len()
        );
        assert!(
            error
                .as_ref()
                .is_some_and(|error| error.to_string().contains("more data than size_bytes")),
            "the overshoot must be reported, got: {error:?}"
        );
    }

    #[test]
    fn rechunk_sends_nothing_when_the_overshoot_arrives_in_a_later_item() {
        // The same hazard reached the other way: each item is legal as it
        // arrives, and the second one completes the declared total exactly. Only
        // holding that completing chunk back until the source confirms it is
        // finished keeps the third item from being discovered too late.
        let (requests, error, _pulled) = collect_rechunked_source(
            (DEFAULT_CHUNK_SIZE * 2) as u64,
            vec![
                chunk(vec![1u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![2u8; DEFAULT_CHUNK_SIZE]),
                chunk(vec![3u8; DEFAULT_CHUNK_SIZE]),
            ],
        );
        let sent: usize = requests.iter().map(|request| request.chunk.len()).sum();
        assert!(
            sent < DEFAULT_CHUNK_SIZE * 2,
            "the declared total must never have been completed on the wire, got {sent} bytes"
        );
        assert!(
            error
                .as_ref()
                .is_some_and(|error| error.to_string().contains("more data than size_bytes")),
            "the overshoot must be reported, got: {error:?}"
        );
    }

    #[test]
    fn rechunk_forgives_a_source_failure_after_the_last_byte_at_a_non_multiple_size() {
        // The other half of the same regression. This forgiveness used to be
        // measured against bytes already *sent*, so it applied only when
        // `size_bytes` was an exact chunk multiple — at any other size the whole
        // tail was still buffered, nothing had been emitted, and the identical
        // source failure was reported as `Io`. `docs/errors-and-retries.md`
        // states the rule without that caveat, and now the code matches it.
        let (requests, error, pulled) =
            collect_rechunked_source(1500, vec![chunk(vec![7u8; 1500]), read_failure()]);
        assert!(
            error.is_none(),
            "every declared byte was read before the failure, got: {error:?}"
        );
        assert_eq!(
            requests
                .iter()
                .map(|request| request.chunk.len())
                .sum::<usize>(),
            1500,
            "the complete file must still be sent"
        );
        assert_eq!(pulled, 2, "nothing may be pulled after the failing item");
    }

    #[test]
    fn rechunk_reports_a_source_failure_on_a_zero_byte_file_rather_than_publishing_an_empty_one() {
        // The one case the "every declared byte is in hand" rule is *not*
        // allowed to forgive, and the reason is data loss rather than symmetry.
        // A zero-byte declaration satisfies that rule before the source is read
        // at all, so forgiving here would publish an empty file — and
        // publishing replaces whatever is stored under that `file_id` in one
        // atomic swap. A caller whose size computation returned 0 by mistake
        // and whose source then failed would destroy the stored file and be
        // told the upload succeeded.
        let (requests, error, pulled) = collect_rechunked_source(0, vec![read_failure()]);
        assert_is_the_source_read_failure(error);
        assert!(
            requests.is_empty(),
            "nothing may be published for an upload that read nothing, got {} requests",
            requests.len()
        );
        assert_eq!(pulled, 1, "nothing may be pulled after the failing item");
    }

    #[test]
    fn rechunk_still_completes_a_zero_byte_file_whose_source_ends_cleanly() {
        // The carve-out is about a *failing* source, not about zero-byte files:
        // a legitimately empty upload still goes out.
        let (requests, error) = collect_rechunked(0, vec![]);
        assert!(
            error.is_none(),
            "an empty source is not a failure: {error:?}"
        );
        assert_eq!(requests.len(), 1, "the metadata request must still go out");
        assert!(requests[0].chunk.is_empty());
    }

    // The other half of the failing-source contract — that the recorded
    // `RociaDbError::Io` wins over the status the *server* returns for the
    // stream that then ended early — needs a server to return a status at all,
    // so it lives in `tests/files.rs` against the in-process one. Against an
    // unreachable host there is nothing to assert: tonic fails the call at the
    // connection stage, before the request stream is ever polled, so the source
    // never gets the chance to fail.

    #[test]
    fn ingest_never_grows_the_buffer_past_one_chunk_for_a_single_oversized_item() {
        // Regression test for the unbounded-buffer bug: a caller who
        // already held the whole file as one in-memory buffer and
        // yielded it as a single stream item used to have that whole item
        // copied into `buffer` by one `Vec::extend` call, defeating the
        // "never buffers more than one outgoing chunk" bound
        // `rechunk_upload_requests` promises.
        // `RechunkState::ingest`/`RechunkState::drain_pending`
        // are exercised directly here (rather than through the async
        // `rechunk_upload_requests` pipeline) so `buffer.len()` can be
        // asserted at every intermediate step, not just inferred from the
        // sizes of the `UploadRequest`s that eventually come out the other
        // end.
        let error_slot: Arc<Mutex<Option<RociaDbError>>> = Arc::new(Mutex::new(None));
        let mut state = RechunkState {
            source: Box::pin(stream::empty::<std::io::Result<Bytes>>()),
            buffer: Vec::new(),
            pending: Bytes::new(),
            pending_offset: 0,
            size_bytes: 0,
            total_written: 0,
            wrote_any: false,
            source_exhausted: false,
            metadata: None,
            error_slot,
        };

        let oversized = Bytes::from(vec![42u8; DEFAULT_CHUNK_SIZE * 5 + 7]);
        state.ingest(oversized.clone());
        assert!(
            state.buffer.len() <= DEFAULT_CHUNK_SIZE,
            "a single ingest() call must never grow the buffer past one output chunk, got {} \
             bytes for a {}-byte item",
            state.buffer.len(),
            oversized.len()
        );

        // Drain exactly like `rechunk_upload_requests`'s `stream::unfold`
        // loop does: emit the buffer once it reaches a full chunk,
        // otherwise pull more of the oversized item's tail out of
        // `pending` — checking the bound holds at every step, not just
        // right after the first `ingest()` call.
        let mut reassembled = Vec::new();
        loop {
            if state.buffer.len() >= DEFAULT_CHUNK_SIZE {
                reassembled.extend(state.buffer.drain(..DEFAULT_CHUNK_SIZE));
            } else if state.has_pending() {
                state.drain_pending();
            } else {
                break;
            }
            assert!(
                state.buffer.len() <= DEFAULT_CHUNK_SIZE,
                "buffer must stay bounded by one output chunk at every step while draining an \
                 oversized item, got {} bytes",
                state.buffer.len()
            );
        }
        reassembled.append(&mut state.buffer);

        assert_eq!(
            reassembled, oversized,
            "draining an oversized item through ingest()/drain_pending() must reproduce it \
             byte-for-byte, with no bytes lost, duplicated, or reordered"
        );
    }

    #[test]
    fn rechunk_reassembles_a_single_oversized_source_item_via_the_full_pipeline() {
        // Companion to the `ingest`/`drain_pending` test above: the same
        // scenario (one source item several times larger than
        // `DEFAULT_CHUNK_SIZE`) driven through the full async
        // `rechunk_upload_requests` pipeline, confirming the fix holds
        // end-to-end and not only at the state-machine level.
        let total = DEFAULT_CHUNK_SIZE * 3 + 12345;
        let oversized: Vec<u8> = (0..total).map(|b| (b % 251) as u8).collect();
        let (requests, error) = collect_rechunked(total as u64, vec![oversized.clone()]);
        assert!(error.is_none(), "unexpected validation error: {error:?}");

        let reassembled: Vec<u8> = requests.iter().flat_map(|r| r.chunk.clone()).collect();
        assert_eq!(
            reassembled, oversized,
            "a single oversized stream item must still reassemble byte-for-byte"
        );

        let (last, all_but_last) = requests.split_last().expect("at least one request");
        for request in all_but_last {
            assert_eq!(
                request.chunk.len(),
                DEFAULT_CHUNK_SIZE,
                "every chunk but the last must still be exactly one full chunk"
            );
        }
        assert!(!last.chunk.is_empty());
        assert!(last.chunk.len() <= DEFAULT_CHUNK_SIZE);
    }

    // -----------------------------------------------------------------------
    // FileMetadata and FileTimestamp
    // -----------------------------------------------------------------------

    /// Seconds from the Unix epoch to 2024-01-01T00:00:00Z: a round anchor
    /// several assertions below count from, and one that is easy to check
    /// against any independent source.
    const NEW_YEAR_2024: i64 = 1_704_067_200;

    /// Parse `raw` and report the instant it names as (whole seconds since the
    /// Unix epoch, nanoseconds into that second), after asserting that all
    /// three public ways of reaching that instant agree.
    ///
    /// The [`std::time::SystemTime`] is cross-checked by reading it back as a
    /// single signed nanosecond count through `duration_since(UNIX_EPOCH)` —
    /// arithmetic the implementation does not share, since it builds the value
    /// by adding or subtracting a `Duration` of seconds *and* nanoseconds.
    fn instant(raw: &str) -> (i64, u32) {
        let (seconds, nanos) =
            parse_rfc3339(raw).unwrap_or_else(|problem| panic!("{raw:?} must parse: {problem}"));
        let timestamp = FileTimestamp::new(raw);
        let unix_nanos = timestamp
            .unix_nanos()
            .expect("a parseable timestamp must report its nanos");
        assert_eq!(
            unix_nanos,
            i128::from(seconds) * 1_000_000_000 + i128::from(nanos),
            "unix_nanos must report the parsed instant, for {raw:?}"
        );
        let system_time = timestamp
            .system_time()
            .expect("a parseable timestamp must resolve to an instant");
        let read_back = match system_time.duration_since(UNIX_EPOCH) {
            Ok(after) => i128::try_from(after.as_nanos()).expect("an instant fits in an i128"),
            Err(before) => {
                -i128::try_from(before.duration().as_nanos()).expect("an instant fits in an i128")
            }
        };
        assert_eq!(
            read_back, unix_nanos,
            "system_time and unix_nanos must name the same instant, for {raw:?}"
        );
        (seconds, nanos)
    }

    /// Whole seconds since the Unix epoch for a timestamp with no fractional
    /// part, asserting there is none.
    fn unix_seconds(raw: &str) -> i64 {
        let (seconds, nanos) = instant(raw);
        assert_eq!(nanos, 0, "{raw:?} carries no fractional seconds");
        seconds
    }

    /// The `Display` of the error `raw` produces, after asserting it is the
    /// documented [`RociaDbError::Decode`] and that both accessors reject it.
    ///
    /// Both, every time: `system_time` and `unix_nanos` run one parser, and a
    /// string one of them accepted while the other refused would be a bug no
    /// single-accessor test could see.
    fn rejection(raw: &str) -> String {
        let timestamp = FileTimestamp::new(raw);
        let error = match timestamp.system_time() {
            Ok(parsed) => panic!("{raw:?} must not parse, got {parsed:?}"),
            Err(error) => error,
        };
        let RociaDbError::Decode { context, .. } = &error else {
            panic!("an unparseable timestamp must be a Decode error, got: {error}");
        };
        assert_eq!(*context, FILE_TIMESTAMP_CONTEXT);
        assert_eq!(*context, "file timestamp");
        assert!(
            timestamp.unix_nanos().is_err(),
            "unix_nanos must reject exactly what system_time rejects, for {raw:?}"
        );
        assert!(
            error.code().is_none(),
            "a timestamp this SDK cannot read is not a gRPC status, got: {error}"
        );
        error.to_string()
    }

    #[test]
    fn file_metadata_carries_the_wire_message_field_for_field() {
        let metadata = FileMetadata::from(StatResponse {
            size_bytes: 4096,
            content_type: "text/csv".to_string(),
            checksum: vec![7u8; CHECKSUM_LEN],
            created_at: "2024-01-01T00:00:00Z".to_string(),
            updated_at: "2024-03-01T12:30:45.5+02:00".to_string(),
        });
        assert_eq!(metadata.size_bytes, 4096);
        assert_eq!(metadata.content_type, "text/csv");
        assert_eq!(metadata.checksum, vec![7u8; CHECKSUM_LEN]);
        // The two timestamps keep the server's own text, byte for byte: the
        // conversion parses nothing.
        assert_eq!(metadata.created_at.as_str(), "2024-01-01T00:00:00Z");
        assert_eq!(metadata.updated_at.as_str(), "2024-03-01T12:30:45.5+02:00");
        assert_eq!(metadata, metadata.clone());
    }

    #[test]
    fn a_timestamp_hands_back_the_servers_own_string_whatever_its_format() {
        // The property that makes the lazy parse safe: text this SDK cannot
        // read is still text the caller can, through both `as_str` and
        // `Display`, and neither has to succeed at parsing to work.
        for raw in [
            "2024-01-01T00:00:00Z",
            "2024-01-01 00:00:00",
            "01/01/2024 00:00:00 CET",
            "1704067200",
            "",
        ] {
            let timestamp = FileTimestamp::new(raw);
            assert_eq!(timestamp.as_str(), raw);
            assert_eq!(timestamp.to_string(), raw);
        }
    }

    #[test]
    fn the_unix_epoch_parses_to_zero() {
        assert_eq!(unix_seconds("1970-01-01T00:00:00Z"), 0);
        assert_eq!(
            FileTimestamp::new("1970-01-01T00:00:00Z")
                .system_time()
                .expect("the epoch must parse"),
            UNIX_EPOCH
        );
    }

    #[test]
    fn known_instants_parse_to_their_documented_unix_seconds() {
        // Four fixed points, each checkable against any other implementation:
        // the epoch's billionth second, the start of 2024, the last second a
        // 32-bit `time_t` can hold, and one right after it.
        assert_eq!(unix_seconds("2001-09-09T01:46:40Z"), 1_000_000_000);
        assert_eq!(unix_seconds("2024-01-01T00:00:00Z"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2038-01-19T03:14:07Z"), 2_147_483_647);
        assert_eq!(unix_seconds("2038-01-19T03:14:08Z"), 2_147_483_648);
        // And the arithmetic inside a day, hour and minute.
        assert_eq!(
            unix_seconds("2024-01-01T01:02:03Z"),
            NEW_YEAR_2024 + 3_600 + 120 + 3
        );
        assert_eq!(
            unix_seconds("2024-12-31T23:59:59Z"),
            NEW_YEAR_2024 + 366 * 86_400 - 1,
            "2024 is a leap year, so it is 366 days long"
        );
    }

    #[test]
    fn leap_years_follow_the_gregorian_rule_rather_than_the_every_fourth_year_one() {
        // The whole point of the century exceptions: 2000 is a leap year
        // because it divides by 400, while 1900 and 2100 are not because they
        // only divide by 100.
        assert_eq!(
            unix_seconds("2000-02-29T00:00:00Z"),
            951_782_400,
            "2000 divides by 400, so it has a 29 February"
        );
        assert_eq!(unix_seconds("2024-02-29T12:00:00Z"), 1_709_208_000);
        for absent in [
            "1900-02-29T00:00:00Z",
            "2100-02-29T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "2024-02-30T00:00:00Z",
        ] {
            assert!(
                rejection(absent).contains("day is out of range"),
                "{absent:?} is not a date"
            );
        }
        // And the rule itself, since the parser is the only thing that reads it.
        assert!(is_leap_year(2000) && is_leap_year(2024) && is_leap_year(1600));
        assert!(!is_leap_year(1900) && !is_leap_year(2100) && !is_leap_year(2023));
        // A day the calendar does have, on both sides of each exception.
        unix_seconds("1900-02-28T00:00:00Z");
        unix_seconds("1900-03-01T00:00:00Z");
    }

    #[test]
    fn month_lengths_are_enforced_and_so_are_the_month_and_day_numbers_themselves() {
        for last_day in [
            "2023-01-31T00:00:00Z",
            "2023-04-30T00:00:00Z",
            "2023-06-30T00:00:00Z",
            "2023-09-30T00:00:00Z",
            "2023-11-30T00:00:00Z",
            "2023-12-31T00:00:00Z",
        ] {
            instant(last_day);
        }
        for overrun in [
            "2023-04-31T00:00:00Z",
            "2023-06-31T00:00:00Z",
            "2023-09-31T00:00:00Z",
            "2023-11-31T00:00:00Z",
            "2023-01-32T00:00:00Z",
            "2023-01-00T00:00:00Z",
        ] {
            assert!(
                rejection(overrun).contains("day is out of range"),
                "{overrun:?} is not a date"
            );
        }
        for month in ["2023-00-10T00:00:00Z", "2023-13-10T00:00:00Z"] {
            assert!(
                rejection(month).contains("month is out of range"),
                "{month:?} is not a date"
            );
        }
        // December of one year and January of the next are one day apart,
        // which is the arithmetic `days_from_civil`'s year shift exists for.
        assert_eq!(
            unix_seconds("2024-01-01T00:00:00Z") - unix_seconds("2023-12-31T00:00:00Z"),
            86_400
        );
    }

    #[test]
    fn fractional_seconds_of_any_length_are_kept_to_the_nanosecond_and_then_truncated() {
        // One, three, six and nine digits: a tenth, a millisecond, a
        // microsecond and a nanosecond, each padded on the right rather than
        // read as its digits.
        assert_eq!(instant("2024-01-01T00:00:00.5Z").1, 500_000_000);
        assert_eq!(instant("2024-01-01T00:00:00.001Z").1, 1_000_000);
        assert_eq!(instant("2024-01-01T00:00:00.000001Z").1, 1_000);
        assert_eq!(instant("2024-01-01T00:00:00.000000001Z").1, 1);
        assert_eq!(instant("2024-01-01T00:00:00.123456789Z").1, 123_456_789);
        // Past the ninth digit the parser truncates rather than rounds, so a
        // value never names an instant later than the one the server wrote.
        assert_eq!(instant("2024-01-01T00:00:00.1234567891Z").1, 123_456_789);
        assert_eq!(
            instant("2024-01-01T00:00:00.999999999999999Z").1,
            999_999_999
        );
        assert_eq!(instant("2024-01-01T00:00:00.0000000009Z").1, 0);
        // The seconds are untouched by any of it, and the fraction survives an
        // offset following it.
        assert_eq!(instant("2024-01-01T00:00:00.250Z").0, NEW_YEAR_2024);
        assert_eq!(
            instant("2024-01-01T02:00:00.250+02:00"),
            (NEW_YEAR_2024, 250_000_000)
        );
    }

    #[test]
    fn an_offset_moves_the_instant_in_both_directions() {
        // Same instant, four spellings: UTC, two positive offsets and one
        // negative half-hour one.
        assert_eq!(unix_seconds("2024-01-01T00:00:00Z"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01T00:00:00z"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01T02:00:00+02:00"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2023-12-31T18:30:00-05:30"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01T14:00:00+14:00"), NEW_YEAR_2024);
        // A zero offset written either way is UTC. RFC 3339 gives `-00:00` the
        // separate meaning "offset unknown"; the instant is the same, and an
        // instant is all this type reports.
        assert_eq!(unix_seconds("2024-01-01T00:00:00+00:00"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01T00:00:00-00:00"), NEW_YEAR_2024);
        // The minutes of an offset count too, and an offset may cross a day.
        assert_eq!(
            unix_seconds("2024-01-01T00:00:00+00:45"),
            NEW_YEAR_2024 - 45 * 60
        );
        assert_eq!(
            unix_seconds("2024-01-01T00:30:00-23:59"),
            NEW_YEAR_2024 + 30 * 60 + 23 * 3_600 + 59 * 60
        );
        // And an offset outside the two-digit range is not one.
        assert!(rejection("2024-01-01T00:00:00+24:00").contains("offset hour is out of range"));
        assert!(rejection("2024-01-01T00:00:00+01:60").contains("offset minute is out of range"));
    }

    #[test]
    fn the_date_and_time_may_be_separated_by_a_space_or_a_lowercase_t() {
        // RFC 3339 spells the separator `T`, allows `t`, and lets an
        // implementation take a space. All three name the same instant here.
        assert_eq!(unix_seconds("2024-01-01T00:00:00Z"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01t00:00:00Z"), NEW_YEAR_2024);
        assert_eq!(unix_seconds("2024-01-01 00:00:00Z"), NEW_YEAR_2024);
        assert_eq!(
            instant("2024-01-01 00:00:00.5z"),
            (NEW_YEAR_2024, 500_000_000),
            "a space separator, a lower-case z and a fraction compose"
        );
        // Nothing else is a separator, however plausible.
        for wrong in [
            "2024-01-01_00:00:00Z",
            "2024-01-01X00:00:00Z",
            "2024-01-01-00:00:00Z",
        ] {
            assert!(rejection(wrong).contains("expected YYYY-MM-DD"));
        }
    }

    #[test]
    fn a_leap_second_is_rejected_rather_than_quietly_moved() {
        // RFC 3339 allows `:60` for a positive leap second, and Unix time has
        // no separate instant to map it to. Refusing says so; the raw string
        // is still there for a caller who needs it.
        let message = rejection("2016-12-31T23:59:60Z");
        assert!(
            message.contains("second is out of range") && message.contains("leap second"),
            "the message must say why 60 is refused, got: {message}"
        );
        assert_eq!(
            FileTimestamp::new("2016-12-31T23:59:60Z").as_str(),
            "2016-12-31T23:59:60Z"
        );
        // The second before it is an ordinary instant.
        unix_seconds("2016-12-31T23:59:59Z");
    }

    #[test]
    fn an_instant_before_1970_lands_before_the_epoch_rather_than_failing() {
        // Supported, not rejected: `UNIX_EPOCH - Duration` on the way out, and
        // a negative number from `unix_nanos`.
        assert_eq!(unix_seconds("1969-12-31T23:59:59Z"), -1);
        assert_eq!(unix_seconds("1969-01-01T00:00:00Z"), -31_536_000);
        assert_eq!(unix_seconds("1900-01-01T00:00:00Z"), -2_208_988_800);
        assert_eq!(unix_seconds("0000-01-01T00:00:00Z"), -62_167_219_200);

        // The sub-second part still counts forward from the second below it,
        // which is the one place the conversion cannot simply negate: half a
        // second before the epoch is -500 000 000 ns, not -1.5 s.
        let (seconds, nanos) = instant("1969-12-31T23:59:59.5Z");
        assert_eq!((seconds, nanos), (-1, 500_000_000));
        assert_eq!(
            FileTimestamp::new("1969-12-31T23:59:59.5Z")
                .unix_nanos()
                .expect("a pre-epoch instant must still report its nanos"),
            -500_000_000
        );
        assert_eq!(
            FileTimestamp::new("1969-12-31T23:59:59.5Z")
                .system_time()
                .expect("a pre-epoch instant must resolve"),
            UNIX_EPOCH - Duration::from_millis(500)
        );
        assert_eq!(
            FileTimestamp::new("1969-12-31T23:59:59Z")
                .system_time()
                .expect("a pre-epoch instant must resolve"),
            UNIX_EPOCH - Duration::from_secs(1)
        );
    }

    #[test]
    fn an_unreadable_timestamp_is_a_decode_error_naming_the_value_and_the_problem() {
        // Every shape a server might plausibly send that this parser does not
        // read, plus the outright garbage. None of them may panic, and none of
        // them may resolve to an instant.
        let cases = [
            ("", "empty"),
            ("2024-01-01", "a date with no time"),
            ("2024-01-01T00:00:00", "a time with no offset"),
            ("2024-01-01 00:00:00", "a SQL-style local time"),
            ("00:00:00Z", "a time with no date"),
            ("1704067200", "epoch seconds"),
            ("2024-1-1T00:00:00Z", "unpadded date fields"),
            ("2024-01-01T0:00:00Z", "an unpadded hour"),
            ("2024-01-01T00:00:00+0200", "an offset without its colon"),
            ("2024-01-01T00:00:00+02", "an offset with no minutes"),
            ("2024-01-01T00:00:00Z ", "a trailing space"),
            ("2024-01-01T00:00:00Zulu", "trailing text"),
            (" 2024-01-01T00:00:00Z", "a leading space"),
            ("2024-01-01T00:00:00.Z", "a decimal point with no digits"),
            ("2024-01-01T00:00:00,5Z", "a comma for the decimal point"),
            ("2024-01-01T24:00:00Z", "hour 24"),
            ("2024-01-01T00:60:00Z", "minute 60"),
            ("2024/01/01T00:00:00Z", "slashes for dashes"),
            ("not a timestamp at all", "prose"),
            ("20240101T000000Z", "the basic ISO 8601 format"),
            ("2024-01-01T00:00:0あZ", "a multi-byte character mid-field"),
            ("+024-01-01T00:00:00Z", "a signed year"),
        ];
        for (raw, what) in cases {
            let message = rejection(raw);
            assert!(
                message.starts_with("failed to decode file timestamp: "),
                "the error must name the context first ({what}), got: {message}"
            );
            assert!(
                message.contains(&format!("{raw:?}")),
                "the error must quote the value it refused ({what}), got: {message}"
            );
        }
        // The two out-of-range cases above are worth pinning by message: they
        // are the ones a reader is most likely to have to act on.
        assert!(rejection("2024-01-01T24:00:00Z").contains("hour is out of range"));
        assert!(rejection("2024-01-01T00:60:00Z").contains("minute is out of range"));
        assert!(
            rejection("2024-01-01T00:00:00").contains("expected a Z or +hh:mm offset"),
            "a missing offset must say so: it is the likeliest thing a server gets wrong"
        );
    }

    #[test]
    fn a_long_unreadable_timestamp_is_quoted_only_up_to_the_documented_bound() {
        // The value is the server's, so its length is not this crate's to
        // trust. A megabyte in `created_at` must not become a megabyte in
        // every log line about it.
        let huge = "9".repeat(4096);
        let message = rejection(&huge);
        assert!(
            message.len() < MAX_QUOTED_TIMESTAMP_CHARS + 200,
            "the message must stay bounded, got {} characters",
            message.len()
        );
        assert!(
            message.contains("(truncated)"),
            "a truncated quote must say so, got: {message}"
        );
        // A timestamp-sized value is quoted whole, with no truncation marker.
        let message = rejection("2024-01-01T00:00:00.123456789012+02:00 (Europe/Paris)");
        assert!(!message.contains("(truncated)"), "got: {message}");
    }

    #[test]
    fn equality_is_over_the_raw_string_rather_than_the_instant() {
        // Documented, and worth pinning: two spellings of one instant are not
        // equal, which is why the docs point at `system_time`/`unix_nanos` for
        // comparisons that are about time.
        let utc = FileTimestamp::new("2024-01-01T00:00:00Z");
        let offset = FileTimestamp::new("2024-01-01T02:00:00+02:00");
        assert_ne!(utc, offset);
        assert_eq!(
            utc.unix_nanos().expect("parses"),
            offset.unix_nanos().expect("parses"),
            "the two must still name the same instant"
        );
        assert_eq!(utc, FileTimestamp::new("2024-01-01T00:00:00Z"));
        assert_eq!(utc, utc.clone());
    }

    #[test]
    fn days_from_civil_agrees_with_known_day_counts_on_both_sides_of_the_epoch() {
        // The date half of the parser, on its own: the epoch is day zero, the
        // day before it is -1, and an era boundary (400 years, 146 097 days)
        // lands where the algorithm's constant says it does.
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(2024, 1, 1), NEW_YEAR_2024 / 86_400);
        assert_eq!(
            days_from_civil(2000, 3, 1) - days_from_civil(1600, 3, 1),
            146_097,
            "one Gregorian era is exactly 146 097 days"
        );
        assert_eq!(
            days_from_civil(2001, 1, 1) - days_from_civil(2000, 1, 1),
            366,
            "2000 is a leap year"
        );
        assert_eq!(
            days_from_civil(1901, 1, 1) - days_from_civil(1900, 1, 1),
            365,
            "1900 is not"
        );
    }
}
