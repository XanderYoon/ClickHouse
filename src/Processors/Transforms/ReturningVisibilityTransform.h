#pragma once

#include <deque>

#include <Core/Block_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>
#include <Processors/Sinks/InsertReturningPublication.h>

namespace DB
{

/// Sits on the `RETURNING` branch, after the storage sink.
/// Forwards a chunk only once `publication` says this insert can no longer drop it.
/// An early finish of the output (`LIMIT`) drops what is still held, so the
/// insert branch of the fork can keep running.
class ReturningVisibilityTransform final : public IProcessor
{
public:
    ReturningVisibilityTransform(SharedHeader header, InsertReturningPublication publication_);

    String getName() const override { return "ReturningVisibility"; }

    Status prepare() override;

private:
    InputPort & input;
    OutputPort & output;
    InsertReturningPublication publication;

    /// Not yet durable.
    std::deque<Chunk> held;
    /// Durable, waiting for the output.
    std::deque<Chunk> ready;
    bool flushed_on_finish = false;
    bool failed = false;

    void dropHeld() { held.clear(); }
    void accept(Chunk chunk);
};

}
