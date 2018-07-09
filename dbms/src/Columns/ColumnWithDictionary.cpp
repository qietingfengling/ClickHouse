#include <Columns/ColumnWithDictionary.h>
#include <Columns/ColumnsNumber.h
#include <DataStreams/ColumnGathererStream.h>
#include <DataTypes/NumberTraits.h>

namespace DB
{

ColumnWithDictionary::ColumnWithDictionary(MutableColumnPtr && column_unique_, MutableColumnPtr && indexes_)
    : column_unique(std::move(column_unique_)), idx(std::move(indexes_))
{
    if (!dynamic_cast<const IColumnUnique *>(column_unique.get()))
        throw Exception("ColumnUnique expected as argument of ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);
}

void ColumnWithDictionary::insertFrom(const IColumn & src, size_t n)
{
    if (!typeid_cast<const ColumnWithDictionary *>(&src))
        throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    auto & src_with_dict = static_cast<const ColumnWithDictionary &>(src);
    size_t position = src_with_dict.getIndexes().getUInt(n);
    insertFromFullColumn(*src_with_dict.getUnique().getNestedColumn(), position);
}

void ColumnWithDictionary::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (!typeid_cast<const ColumnWithDictionary *>(&src))
        throw Exception("Expected ColumnWithDictionary, got" + src.getName(), ErrorCodes::ILLEGAL_COLUMN);

    auto & src_with_dict = static_cast<const ColumnWithDictionary &>(src);

    if (&src_with_dict.getUnique() == &getUnique())
    {
        /// Dictionary is shared with src column. Insert only indexes.
        idx.insertPositionsRange(src_with_dict.getIndexes(), start, length);
    }
    else
    {
        /// TODO: Support native insertion from other unique column. It will help to avoid null map creation.
        auto src_nested = src_with_dict.getUnique().getNestedColumn();
        auto inserted_idx = getUnique().uniqueInsertRangeFrom(*src_nested, 0, src_nested->size());
        auto indexes = inserted_idx->index(*src_with_dict.getIndexes().cut(start, length), 0);
        idx.insertPositionsRange(*indexes, 0, length);
    }
}

void ColumnWithDictionary::insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length)
{
    auto inserted_indexes = getUnique().uniqueInsertRangeFrom(src, start, length);
    idx.insertPositionsRange(*inserted_indexes, 0, length);
}

void ColumnWithDictionary::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

MutableColumnPtr ColumnWithDictionary::cloneResized(size_t size) const
{
    auto unique_ptr = column_unique;
    return ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), getIndexes().cloneResized(size));
}

int ColumnWithDictionary::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto & column_with_dictionary = static_cast<const ColumnWithDictionary &>(rhs);
    size_t n_index = getIndexes().getUInt(n);
    size_t m_index = column_with_dictionary.getIndexes().getUInt(m);
    return getUnique().compareAt(n_index, m_index, *column_with_dictionary.column_unique, nan_direction_hint);
}

void ColumnWithDictionary::getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const
{
    if (limit == 0)
        limit = size();

    size_t unique_limit = std::min(limit, getUnique().size());
    Permutation unique_perm;
    getUnique().getNestedColumn()->getPermutation(reverse, unique_limit, nan_direction_hint, unique_perm);

    /// TODO: optimize with sse.

    /// Get indexes per row in column_unique.
    std::vector<std::vector<size_t>> indexes_per_row(getUnique().size());
    size_t indexes_size = getIndexes().size();
    for (size_t row = 0; row < indexes_size; ++row)
        indexes_per_row[getIndexes().getUInt(row)].push_back(row);

    /// Replicate permutation.
    size_t perm_size = std::min(indexes_size, limit);
    res.resize(perm_size);
    size_t perm_index = 0;
    for (size_t row = 0; row < indexes_size && perm_index < perm_size; ++row)
    {
        const auto & row_indexes = indexes_per_row[unique_perm[row]];
        for (auto row_index : row_indexes)
        {
            res[perm_index] = row_index;
            ++perm_index;

            if (perm_index == perm_size)
                break;
        }
    }
}

std::vector<MutableColumnPtr> ColumnWithDictionary::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto columns = getIndexes().scatter(num_columns, selector);
    for (auto & column : columns)
    {
        auto unique_ptr = column_unique;
        column = ColumnWithDictionary::create((*std::move(unique_ptr)).mutate(), std::move(column));
    }

    return columns;
}

void ColumnWithDictionary::setUnique(const ColumnPtr & unique)
{
    if (!dynamic_cast<const IColumnUnique *>(column_unique.get()))
        throw Exception("ColumnUnique expected as argument of ColumnWithDictionary.", ErrorCodes::ILLEGAL_COLUMN);

    if (!empty())
        throw Exception("Can't set ColumnUnique for ColumnWithDictionary because is't not empty.",
                        ErrorCodes::LOGICAL_ERROR);

    column_unique = unique;
}


ColumnWithDictionary::Index::Index() : positions(ColumnUInt8::create()), size_of_type(sizeof(UInt8)) {}

ColumnWithDictionary::Index::Index(MutableColumnPtr && positions) : positions(std::move(positions))
{
    updateSizeOfType();
}

ColumnWithDictionary::Index::Index(ColumnPtr positions) : positions(std::move(positions))
{
    updateSizeOfType();
}

template <typename Callback>
void ColumnWithDictionary::Index::callForType(Callback && callback, size_t size_of_type)
{
    switch (size_of_type)
    {
        case sizeof(UInt8): { callback(UInt8()); break; }
        case sizeof(UInt16): { callback(UInt16()); break; }
        case sizeof(UInt32): { callback(UInt32()); break; }
        case sizeof(UInt64): { callback(UInt64()); break; }
        default: {
            throw Exception("Unexpected size of index type for ColumnWithDictionary: " + toString(size_of_type),
                            ErrorCodes::LOGICAL_ERROR);
        }
    }
}

size_t ColumnWithDictionary::Index::getSizeOfIndexType(const IColumn & column, size_t hint)
{
    auto checkFor = [&](auto type) { return typeid_cast<const ColumnVector<decltype(type)> *>(&column) != nullptr; };
    auto tryGetSizeFor = [&](auto type) -> size_t { return checkFor(type) ? sizeof(decltype(type)) : 0; };

    if (hint)
    {
        size_t size = 0;
        callForType([&](auto type) { size = tryGetSizeFor(type); }, hint);

        if (size)
            return size;
    }

    if (auto size = tryGetSizeFor(UInt8()))
        return size;
    if (auto size = tryGetSizeFor(UInt16()))
        return size;
    if (auto size = tryGetSizeFor(UInt32()))
        return size;
    if (auto size = tryGetSizeFor(UInt64()))
        return size;

    throw Exception("Unexpected indexes type for ColumnWithDictionary. Expected ColumnUInt, got " + column.getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
}

template <typename IndexType>
ColumnVector<IndexType>::Container & ColumnWithDictionary::Index::getPositionsData()
{
    auto * positions_ptr = typeid_cast<ColumnVector<IndexType> *>(positions->assumeMutable().get());
    if (!positions_ptr)
        throw Exception("Invalid indexes type for ColumnWithDictionary. Expected "
                        + demangle(typeid(typename ColumnVector<IndexType>).name())
                        + ", got " + positions->getName(), ErrorCodes::LOGICAL_ERROR);

    return positions_ptr->getData();
}

template <typename IndexType>
void ColumnWithDictionary::Index::convertPositions()
{
    auto convert = [&](auto x)
    {
        using CurIndexType = typeof(x);
        auto & data = getPositionsData<CurIndexType>();

        if (sizeof(CurIndexType) != sizeof(IndexType))
        {
            size_t size = data.size();
            auto new_positions = ColumnVector<IndexType>::create(size);
            auto & new_data = new_positions->getData();

            /// TODO: Optimize with SSE?
            for (size_t i = 0; i < size; ++i)
                new_data[i] = data[i];

            positions = std::move(new_positions);
            size_of_type = sizeof(IndexType);
        }
    };

    callForType(std::move(convert), size_of_type);
}

void ColumnWithDictionary::Index::expandType()
{
    auto expand = [&](auto type)
    {
        using CurIndexType = decltype(type);
        auto next_size = NumberTraits::nextSize(sizeof(CurIndexType));
        if (next_size == sizeof(CurIndexType))
            throw Exception("Can't expand indexes type for ColumnWithDictionary from type: "
                            + demangle(typeid(CurIndexType).name()), ErrorCodes::LOGICAL_ERROR);

        using NewIndexType = typename NumberTraits::Construct<false, false, next_size>::Type;
        convertPositions<NewIndexType>();
    };

    callForType(std::move(expand), size_of_type);
}

UInt64 ColumnWithDictionary::Index::getMaxPositionForCurrentType() const
{
    UInt64 value = 0;
    callForType([&](auto type) { value = std::numeric_limits<decltype(type)>::max(); }, size_of_type);
    return value;
}

void ColumnWithDictionary::Index::insertPosition(UInt64 position)
{
    while (position > getMaxPositionForCurrentType())
        expandType();

    positions->insert(UInt64(position));
}

void ColumnWithDictionary::Index::insertPositionsRange(const IColumn & column, size_t offset, size_t limit)
{
    auto insertForType = [&](auto type)
    {
        using ColumnType = decltype(type);
        const auto * column_ptr = typeid_cast<const ColumnVector<ColumnType> *>(&column);

        if (!column_ptr)
            return false;

        if (size_of_type < sizeof(ColumnType))
            convertPositions<ColumnType>();

        if (size_of_type == sizeof(ColumnType))
            positions->insertRangeFrom(column, offset, limit);
        else
        {
            auto copy = [&](auto cur_type)
            {
                using CurIndexType = decltype(cur_type);
                auto & positions_data = getPositionsData<CurIndexType>();
                const auto & column_data = column_ptr->getData();

                size_t size = positions_data.size();
                positions_data.resize(size + limit);

                for (size_t i = 0; i < limit; ++i)
                    positions_data[size + i] = column_data[offset + i];
            };

            callForType(std::move(copy), size_of_type);
        }

        return true;
    };

    if (!insertForType(UInt8()) &&
        !insertForType(UInt16()) &&
        !insertForType(UInt32()) &&
        !insertForType(UInt64()))
        throw Exception("Invalid column for ColumnWithDictionary index. Expected ColumnUInt, got " + column.getName(),
                        ErrorCodes::ILLEGAL_COLUMN);
}

}
