package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbField;
    Type gbFieldType;
    int aField;
    Op what;
    TupleDesc td;

    private final HashMap<Field, Integer> resultMap;
    private final HashMap<Field, Integer> cntMap;

    /**
     * Aggregate constructor
     * 
     * @param gbField
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aField
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        this.resultMap = new HashMap<>();
        this.cntMap = new HashMap<>();

        if (what.equals(Op.SC_AVG) || what.equals(Op.SUM_COUNT))
            throw new IllegalArgumentException("@IntegerAggregator: Not implemented.");
        if (gbField == NO_GROUPING) td = new TupleDesc(new Type[]{Type.INT_TYPE});
        else td = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = (gbField == NO_GROUPING) ? null : tup.getField(gbField);
        Integer value = ((IntField) tup.getField(aField)).getValue();
        Integer result = resultMap.get(groupKey);
        Integer cnt = cntMap.get(groupKey);
        switch (what) {
            case MIN: {
                if (result == null) resultMap.put(groupKey, value);
                else resultMap.put(groupKey, Math.min(result, value));
                break;
            }
            case MAX: {
                if (result == null) resultMap.put(groupKey, value);
                else resultMap.put(groupKey, Math.max(result, value));
                break;
            }
            case SUM: {
                if (result == null) resultMap.put(groupKey, value);
                else resultMap.put(groupKey, result + value);
                break;
            }
            case AVG: {
                if (result == null) {
                    resultMap.put(groupKey, value);
                    cntMap.put(groupKey, 1);
                }
                else {
                    resultMap.put(groupKey, result + value);
                    cntMap.put(groupKey, cnt + 1);
                }
                break;
            }
            case COUNT: {
                if (cnt == null) cntMap.put(groupKey, 1);
                else cntMap.put(groupKey, cnt + 1);
                break;
            }
            default:
                throw new IllegalArgumentException("@mergeTupleIntoGroup: Unknown operator");
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        ArrayList<Tuple> tupleList = new ArrayList<>();
        Set<Field> candidate;
        if (what == Op.COUNT) candidate = cntMap.keySet();
        else candidate = resultMap.keySet();
        for (Field groupKey : candidate) {
            Field result;
            if (what == Op.COUNT) result = new IntField(cntMap.get(groupKey));
            else if (what == Op.AVG) result = new IntField(resultMap.get(groupKey) / cntMap.get(groupKey));
            else result = new IntField(resultMap.get(groupKey));

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
