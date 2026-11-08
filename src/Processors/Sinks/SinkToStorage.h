#pragma once
#include <Storages/TableLockHolder.h>
#include <Processors/Sinks/InsertReturningPublication.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

namespace DB
{

class Context;

/// Sink which is returned from Storage::write.
class SinkToStorage : public ExceptionKeepingTransform
{
/// PartitionedSink owns nested sinks.
friend class PartitionedSink;
friend class DeltaLakePartitionedSink;

public:
    explicit SinkToStorage(SharedHeader header);

    const Block & getHeader() const { return inputs.front().getHeader(); }
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }
    void addInterpreterContext(std::shared_ptr<const Context> context) { interpreter_context.emplace_back(std::move(context)); }

    virtual void setHasDependentMaterializedViews(bool /*has_dependent_views*/) {}

    /// The chunk is visible to `RETURNING` only after `onFinish`.
    /// Sinks that publish earlier override this. See `InsertReturningPublication`.
    virtual InsertReturningPublication getInsertReturningPublication() const
    {
        return InsertReturningPublication::OnSuccessfulFinish;
    }

protected:
    virtual void consume(Chunk & chunk) = 0;

private:
    std::vector<TableLockHolder> table_locks;
    std::vector<std::shared_ptr<const Context>> interpreter_context;

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    Chunk cur_chunk;
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage final : public SinkToStorage
{
public:
    using SinkToStorage::SinkToStorage;
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk &) override {}

    /// `consume` discards the chunk. A later failure does not restore it.
    InsertReturningPublication getInsertReturningPublication() const override
    {
        return InsertReturningPublication::OnConsume;
    }
};

using SinkPtr = std::shared_ptr<SinkToStorage>;
}
