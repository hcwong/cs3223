package qp.utils;

import java.util.List;
import java.util.Comparator;

import static qp.utils.Tuple.compareTuples;

/**
 * Comparator to help with Tuple sorting based on a single attribute.
 */
public class TupleComparator implements Comparator<Tuple> {

    private List<Integer> indexes;
    private boolean isReverse;

    /**
     * Takes in a list of indexes. If tied on first index, use second index to tiebreak and so on.
     * @param indexes List<Integer>
     */
    public TupleComparator(List<Integer> indexes) {
       this.indexes = indexes;
       this.isReverse = false;
    }

    /**
     * Takes in a list of indexes. If tied on first index, use second index to tiebreak and so on.
     * This second comparator is if we want it to be reversed
     * @param indexes List<Integer>
     */
    public TupleComparator(List<Integer> indexes, boolean isReverse) {
        this.indexes = indexes;
        this.isReverse = isReverse;
    }


    /**
     * Comparing tuples using the static methods in Tuple class.
     */
    @Override
    public int compare(Tuple left, Tuple right) {
//        For some reason the guard clause below does not work
//        if (left.data().size() >= this.index || right.data().size() >= this.index) {
//            System.out.println(String.format("%d, %d, %d", left.data().size(), right.data().size(), this.index));
//            System.out.println("Tuple: Comparator index exceeds bounds");
//            System.exit(1);
//        }

        for (Integer i: indexes) {
            int result = compareTuples(left, right, i);
            if (result != 0)
                return this.isReverse ? result * -1 : result;
        }
        return 0;
    }
}
