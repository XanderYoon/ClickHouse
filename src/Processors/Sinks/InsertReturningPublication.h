#pragma once

#include <cstdint>

namespace DB
{

/// When a chunk of `INSERT ... RETURNING` may be shown to the client.
/// `consume` returning is not enough: several sinks publish later and drop
/// the write if the insert is cancelled.
enum class InsertReturningPublication : uint8_t
{
    /// Durable when `consume` returns. Nothing later in this insert removes it.
    OnConsume,

    /// Durable once the next chunk is consumed, or when `onFinish` runs.
    /// `MergeTree` commits the previous block at the start of the next
    /// `consume` and commits the last block in `onFinish`. The block still
    /// in `delayed_chunk` is cancelled with the sink.
    OnNextChunk,

    /// Durable only if `onFinish` returns. Cancellation publishes nothing.
    /// `Memory`, `Log`, and file-backed sinks work this way.
    OnSuccessfulFinish,
};

}
