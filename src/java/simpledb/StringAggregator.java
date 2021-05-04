package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbField;
    Type gbFieldType;
    int aField;
    Op what;
    TupleDesc td;

    private final HashMap<Field, Integer> cntMap;

    /**
     * Aggregate constructor
     * @param gbField the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aField the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        this.cntMap = new HashMap<>();

        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("@StringAggregator: Not implemented.");
        if (gbField == NO_GROUPING) td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (gbField == NO_GROUPING) ? null : tup.getField(gbField);
        cntMap.merge(groupKey, 1, Integer::sum);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tupleList = new ArrayList<>();
        for (Field groupKey : cntMap.keySet()) {
            Field result = new IntField(cntMap.get(groupKey));
            Tuple t = new Tuple(td);
            if (gbField == NO_GROUPING) t.setField(0, result);
            else {
                t.setField(0, groupKey);
                t.setField(1, result);
            }
            tupleList.add(t);
        }
        return new TupleIterator(td, tupleList);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

}
