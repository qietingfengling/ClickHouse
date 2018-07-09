#pragma once
#include <Columns/IColumn.h>
#include <Columns/IColumnUnique.h>
#include <Common/typeid_cast.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include "ColumnsNumber.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

class ColumnWithDictionary final : public COWPtrHelper<IColumn, ColumnWithDictionary>
{
    friend class COWPtrHelper<IColumn, ColumnWithDictionary>;

    ColumnWithDictionary(MutableColumnPtr && column_unique, MutableColumnPtr && indexes);
    ColumnWithDictionary(const ColumnWithDictionary & other) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWPtrHelper<IColumn, ColumnWithDictionary>;
    static Ptr create(const ColumnPtr & column_unique_, const ColumnPtr & indexes_)
    {
        return ColumnWithDictionary::create(column_unique_->assumeMutable(), indexes_->assumeMutable());
    }

    template <typename ... Args, typename = typename std::enable_if<IsMutableColumns<Args ...>::value>::type>
    static MutablePtr create(Args &&... args) { return Base::create(std::forward<Args>(args)...); }


    std::string getName() const override { return "ColumnWithDictionary"; }
    const char * getFamilyName() const override { return "ColumnWithDictionary"; }

    ColumnPtr convertToFullColumn() const { return getUnique().getNestedColumn()->index(getIndexes(), 0); }
    ColumnPtr convertToFullColumnIfWithDictionary() const override { return convertToFullColumn(); }

    MutableColumnPtr cloneResized(size_t size) const override;
    size_t size() const override { return getIndexes().size(); }

    Field operator[](size_t n) const override { return (*column_unique)[getIndexes().getUInt(n)]; }
    void get(size_t n, Field & res) const override { column_unique->get(getIndexes().getUInt(n), res); }

    StringRef getDataAt(size_t n) const override { return column_unique->getDataAt(getIndexes().getUInt(n)); }
    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return column_unique->getDataAtWithTerminatingZero(getIndexes().getUInt(n));
    }

    UInt64 get64(size_t n) const override { return column_unique->get64(getIndexes().getUInt(n)); }
    UInt64 getUInt(size_t n) const override { return column_unique->getUInt(getIndexes().getUInt(n)); }
    Int64 getInt(size_t n) const override { return column_unique->getInt(getIndexes().getUInt(n)); }
    bool isNullAt(size_t n) const override { return column_unique->isNullAt(getIndexes().getUInt(n)); }
    ColumnPtr cut(size_t start, size_t length) const override
    {
        return ColumnWithDictionary::create(column_unique, getIndexes().cut(start, length));
    }

    void insert(const Field & x) override { idx.insertPosition(getUnique().uniqueInsert(x)); }

    void insertFrom(const IColumn & src, size_t n) override;
    void insertFromFullColumn(const IColumn & src, size_t n) { idx.insertPosition(getUnique().uniqueInsertFrom(src, n)); }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
    void insertRangeFromFullColumn(const IColumn & src, size_t start, size_t length);

    void insertData(const char * pos, size_t length) override
    {
        idx.insertPosition(getUnique().uniqueInsertData(pos, length));
    }

    void insertDataWithTerminatingZero(const char * pos, size_t length) override
    {
        idx.insertPosition(getUnique().uniqueInsertDataWithTerminatingZero(pos, length));
    }

    void insertDefault() override { idx.insertPosition(getUnique().getDefaultValueIndex()); }

    void popBack(size_t n) override { idx.getPositions()->popBack(n); }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        return getUnique().serializeValueIntoArena(getIndexes().getUInt(n), arena, begin);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        const char * new_pos;
        idx.insertPosition(getUnique().uniqueDeserializeAndInsertFromArena(pos, new_pos));
        return new_pos;
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        return getUnique().updateHashWithValue(getIndexes().getUInt(n), hash);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override
    {
        return ColumnWithDictionary::create(column_unique, getIndexes().filter(filt, result_size_hint));
    }

    ColumnPtr permute(const Permutation & perm, size_t limit) const override
    {
        return ColumnWithDictionary::create(column_unique, getIndexes().permute(perm, limit));
    }

    ColumnPtr index(const IColumn & indexes_, size_t limit) const override
    {
        return ColumnWithDictionary::create(column_unique, getIndexes().index(indexes_, limit));
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    ColumnPtr replicate(const Offsets & offsets) const override
    {
        return ColumnWithDictionary::create(column_unique, getIndexes().replicate(offsets));
    }

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override ;
    void getExtremes(Field & min, Field & max) const override { return column_unique->getExtremes(min, max); }

    void reserve(size_t n) override { idx.getPositions()->reserve(n); }

    size_t byteSize() const override { return idx.getPositions()->byteSize() + column_unique->byteSize(); }
    size_t allocatedBytes() const override { return idx.getPositions()->allocatedBytes() + column_unique->allocatedBytes(); }

    void forEachSubcolumn(ColumnCallback callback) override
    {
        callback(column_unique);
        callback(idx.getPositions());
    }

    bool valuesHaveFixedSize() const override { return column_unique->valuesHaveFixedSize(); }
    bool isFixedAndContiguous() const override { return column_unique->isFixedAndContiguous(); }
    size_t sizeOfValueIfFixed() const override { return column_unique->sizeOfValueIfFixed(); }
    bool isNumeric() const override { return column_unique->isNumeric(); }

    const IColumnUnique & getUnique() const { return static_cast<const IColumnUnique &>(*column_unique->assumeMutable()); }
    IColumnUnique & getUnique() { return static_cast<IColumnUnique &>(*column_unique->assumeMutable()); }
    ColumnPtr getUniquePtr() const { return column_unique; }

    IColumn & getIndexes() { return idx.getPositions()->assumeMutableRef(); }
    const IColumn & getIndexes() const { return *idx.getPositions(); }
    const ColumnPtr & getIndexesPtr() const { return idx.getPositions(); }

    ///void setIndexes(MutableColumnPtr && indexes_) { indexes = std::move(indexes_); }

    /// Set shared ColumnUnique for empty column with dictionary.
    void setUnique(const ColumnPtr & unique);

    bool withDictionary() const override { return true; }

private:

    class Index
    {
    public:
        Index();
        Index(const Index & other) = default;
        explicit Index(MutableColumnPtr && positions);
        explicit Index(ColumnPtr positions);

        const ColumnPtr & getPositions() const { return positions; }
        ColumnPtr & getPositions() { return positions; }
        void insertPosition(UInt64 position);
        void insertPositionsRange(const IColumn & column, size_t offset, size_t limit);

        UInt64 getMaxPositionForCurrentType() const;

        static size_t getSizeOfIndexType(const IColumn & column, size_t hint);

    private:
        ColumnPtr positions;
        size_t size_of_type = 0;

        void updateSizeOfType() { size_of_type = getSizeOfIndexType(*positions, size_of_type); }
        void expandType();

        template <typename IndexType>
        ColumnVector<IndexType>::Container & getPositionsData();

        template <typename IndexType>
        void convertPositions();

        template <typename Callback>
        static void callForType(Callback && callback, size_t size_of_type);
    };

    ColumnPtr column_unique;
    Index idx;
};



}
