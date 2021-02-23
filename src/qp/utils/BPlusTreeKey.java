package qp.utils;

import java.io.Serializable;
import java.util.List;

/**
 * This class is the key for the B+ Tree.
 * It contains a list of indexes
 */
public class BPlusTreeKey implements Comparable<BPlusTreeKey>, Serializable {
    List<Object> indexes;

    public BPlusTreeKey(List<Object> indexes) {
        this.indexes = indexes;
    }

    public List<Object> getIndexes() {
        return indexes;
    }

    @Override
    public int compareTo(BPlusTreeKey others) {
        if (indexes.size() != others.getIndexes().size()) {
            System.out.println("BPlusTree Keys do not match");
        }

        for (int i = 0; i < indexes.size(); i++) {
            Object leftdata = indexes.get(i);
            Object rightdata = others.getIndexes().get(i);
            int result = 0;
            if (leftdata instanceof Integer) {
                result = ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                result = ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                result =  ((Float) leftdata).compareTo((Float) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples");
                System.exit(1);
            }

            if (result != 0) {
                return result;
            }
        }

        return 0;
    }
}
