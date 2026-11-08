#include <Processors/Transforms/ReturningVisibilityTransform.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReturningVisibilityTransform::ReturningVisibilityTransform(SharedHeader header, InsertReturningPublication publication_)
    : IProcessor({header}, {header})
    , input(inputs.front())
    , output(outputs.front())
    , publication(publication_)
{
}

void ReturningVisibilityTransform::accept(Chunk chunk)
{
    if (publication == InsertReturningPublication::OnConsume)
    {
        ready.push_back(std::move(chunk));
        return;
    }

    if (publication == InsertReturningPublication::OnNextChunk)
    {
        /// The sink commits chunk N-1 before it pushes chunk N.
        if (!held.empty())
        {
            ready.push_back(std::move(held.front()));
            held.pop_front();
        }
        held.push_back(std::move(chunk));
        return;
    }

    if (publication == InsertReturningPublication::OnSuccessfulFinish)
    {
        held.push_back(std::move(chunk));
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected INSERT RETURNING publication mode");
}

IProcessor::Status ReturningVisibilityTransform::prepare()
{
    /// `LIMIT` in the `RETURNING` query finishes this output. Drop the held
    /// chunk and close the input so the fork can keep feeding the insert.
    if (output.isFinished())
    {
        dropHeld();
        ready.clear();
        input.close();
        return Status::Finished;
    }

    /// A `CHECK` failure makes `ForkProcessor` throw out of `pull` and cancel
    /// the pipeline, instead of delivering the exception here. The held chunk
    /// is the one the sink has not committed.
    if (isCancelled())
    {
        dropHeld();
        ready.clear();
        input.close();
        output.finish();
        return Status::Finished;
    }

    if (failed)
    {
        /// The exception is still in the output until the consumer pulls it.
        dropHeld();
        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        input.close();
        output.finish();
        return Status::Finished;
    }

    if (!ready.empty())
    {
        if (!output.canPush())
        {
            input.setNotNeeded();
            return Status::PortFull;
        }

        output.push(std::move(ready.front()));
        ready.pop_front();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        /// `onFinish` has already published the last chunk: the sink finishes
        /// its output only after `onFinish` returns.
        if (!flushed_on_finish)
        {
            flushed_on_finish = true;
            while (!held.empty())
            {
                ready.push_back(std::move(held.front()));
                held.pop_front();
            }
        }

        if (!ready.empty())
            return Status::Ready;

        output.finish();
        return Status::Finished;
    }

    /// `OnSuccessfulFinish` must read the whole input before it can forward
    /// anything. Waiting for the output first would stall the sink.
    if (publication != InsertReturningPublication::OnSuccessfulFinish && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto data = input.pullData();
    if (data.exception)
    {
        dropHeld();
        failed = true;
        output.pushException(std::move(data.exception));
        return Status::PortFull;
    }

    accept(std::move(data.chunk));
    return Status::Ready;
}

}
