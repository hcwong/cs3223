package qp.utils;

import java.util.Comparator;

import static qp.utils.Tuple.compareTuples;

/**
 * Comparator to help with Tuple sorting based on a single attribute.
 */
public class TupleComparator implements Comparator<Tuple> {

    private int index;

    // Index to sort the tuple by.
    public TupleComparator(int index) {
        this.index = index;
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
        return compareTuples(left, right, this.index);
    }
}
