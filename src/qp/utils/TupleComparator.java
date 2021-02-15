package qp.utils;

import java.util.Comparator;
import java.io.*;

/**
 * Comparator to help with Tuple sorting based on a single attribute.
 */
public class TupleComparator implements Comparator<Tuple> {

    private int index;

    public Comparator(int index) {
        this.index = index;
    }

    /**
     * Comparing tuples using the static methods in Tuple class.
     */
    @Override
    public int compare(Tuple left, Tuple right) {
        if (left.data().size() >= this.index || right.data().size() > this.index) {
            System.out.println("Tuple: Comparator index exceeds bounds");
            System.exit(1);
            return 0;
        }

        return compareTuples(left, right, this.index);
    }
}
