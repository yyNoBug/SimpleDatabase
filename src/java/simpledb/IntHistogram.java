package simpledb;

import static java.lang.Math.max;
import static java.lang.Math.min;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int buckets;
    int min;
    int max;
    int[] bucketCnt;
    int bucketWidth;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = min(buckets, max - min + 1);
    	this.min = min;
    	this.max = max;
    	this.bucketCnt = new int[this.buckets];
    	this.bucketWidth = (int) Math.round((max - min + 1) * 1.0 / this.buckets);
    }

    private int findBucket(int v) {
        int vv = v - this.min;
        return min(vv / bucketWidth, buckets - 1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	bucketCnt[findBucket(v)]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v < this.min || v > this.max) {
            switch (op) {
                case GREATER_THAN_OR_EQ: case GREATER_THAN:
                    return v < this.min ? 1.0 : 0.0;
                case LESS_THAN_OR_EQ: case LESS_THAN:
                    return v < this.min ? 0.0 : 1.0;
                case NOT_EQUALS:
                    return 1.0;
                case EQUALS:
                    return 0.0;
            }
        }
        int no = findBucket(v);
        int left = 0, right = 0, total = 0;
        for (int i = 0; i < no; ++i) left += bucketCnt[i];
        for (int i = no + 1; i < buckets; ++i) right += bucketCnt[i];
        total = left + right + bucketCnt[no];
        int bucketStart = no * bucketWidth + min;
        int bucketEnd = (no == buckets - 1) ? max + 1 : (no + 1) * bucketWidth + min;
        int width = bucketEnd - bucketStart;

        switch (op) {
            case EQUALS:
                return 1.0 * bucketCnt[no] / width / total;
            case NOT_EQUALS:
                return 1.0 - 1.0 * bucketCnt[no] / width / total;
            case LESS_THAN:
                return (left + 1.0 * bucketCnt[no] * (v - bucketStart) / width) / total;
            case GREATER_THAN:
                return (right + 1.0 * bucketCnt[no] * (bucketEnd - v - 1) / width) / total;
            case LESS_THAN_OR_EQ:
                return 1.0 - (right + 1.0 * bucketCnt[no] * (bucketEnd - v - 1) / width) / total;
            case GREATER_THAN_OR_EQ:
                return 1.0 - (left + 1.0 * bucketCnt[no] * (v - bucketStart) / width) / total;
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append("IntHistogram").append(" ,max: ").append(max)
        .append(" ,min: ").append(min)
        .append(" ,buckets: ").append(buckets);
        return s.toString();
    }
}
