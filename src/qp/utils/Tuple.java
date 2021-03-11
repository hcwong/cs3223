/**
 * Tuple container class
 **/

package qp.utils;

import java.io.*;
import java.lang.StringBuilder;
import java.util.*;

/**
 * Tuple - a simple object which holds an ArrayList of data
 */
public class Tuple implements Serializable {

    public ArrayList<Object> _data;

    public Tuple(ArrayList<Object> d) {
        _data = d;
    }

    /**
     * Accessor for data
     */
    public ArrayList<Object> data() {
        return _data;
    }

    public Object dataAt(int index) {
        return _data.get(index);
    }

    /**
     * Checks whether the join condition is satisfied or not with one condition
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, int leftindex, int rightindex) {
        Object leftData = dataAt(leftindex);
        Object rightData = right.dataAt(rightindex);
        if (leftData.equals(rightData))
            return true;
        else
            return false;
    }

    /**
     * Checks whether the join condition is satisfied or not with multiple conditions
     * * before performing actual join operation
     **/
    public boolean checkJoin(Tuple right, ArrayList<Integer> leftindex, ArrayList<Integer> rightindex) {
        if (leftindex.size() != rightindex.size())
            return false;
        for (int i = 0; i < leftindex.size(); ++i) {
            Object leftData = dataAt(leftindex.get(i));
            Object rightData = right.dataAt(rightindex.get(i));
            if (!leftData.equals(rightData)) {
                return false;
            }
        }
        return true;
    }

    public boolean checkJoin(Tuple right,
                             ArrayList<Integer> leftindex,
                             ArrayList<Integer> rightindex,
                             ArrayList<Condition> conditions
    ) {
        if (leftindex.size() != rightindex.size())
            return false;
        for (int i = 0; i < leftindex.size(); ++i) {
            Object leftData = dataAt(leftindex.get(i));
            Object rightData = right.dataAt(rightindex.get(i));
            Condition cond = conditions.get(i);

            switch(cond.getExprType()) {
            case Condition.LESSTHAN:
                if (compareTuples(this, right, i, i) >= 0) {
                    return false;
                }
                break;
            case Condition.GREATERTHAN:
                if (compareTuples(this, right, i, i) <= 0) {
                    return false;
                }
                break;
            case Condition.LTOE:
                if (compareTuples(this, right, i, i) > 0) {
                    return false;
                }
                break;
            case Condition.GTOE:
                if (compareTuples(this, right, i, i) < 0) {
                    return false;
                }
                break;
            case Condition.EQUAL:
                if (!leftData.equals(rightData)) {
                    return false;
                }
                break;
            case Condition.NOTEQUAL:
                if (leftData.equals(rightData)) {
                    return false;
                }
                break;
            default:
                System.out.println("Nested Loop: Switch statement cannot recognise case");
                System.exit(1);
            }
        }
        return true;
    }

    /**
     * Joining two tuples without duplicate column elimination
     **/
    public Tuple joinWith(Tuple right) {
        ArrayList<Object> newData = new ArrayList<>(this.data());
        newData.addAll(right.data());
        return new Tuple(newData);
    }

    /**
     * Compare two tuples in the same table on given attribute
     **/
    public static int compareTuples(Tuple left, Tuple right, int index) {
        return compareTuples(left, right, index, index);
    }

    /**
     * Comparing tuples in different tables, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, int leftIndex, int rightIndex) {
        Object leftdata = left.dataAt(leftIndex);
        Object rightdata = right.dataAt(rightIndex);
        if (leftdata instanceof Integer) {
            return ((Integer) leftdata).compareTo((Integer) rightdata);
        } else if (leftdata instanceof String) {
            return ((String) leftdata).compareTo((String) rightdata);
        } else if (leftdata instanceof Float) {
            return ((Float) leftdata).compareTo((Float) rightdata);
        } else {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
    }

    /**
     * Comparing tuples in different tables with multiple conditions, used for join condition checking
     **/
    public static int compareTuples(Tuple left, Tuple right, ArrayList<Integer> leftIndex, ArrayList<Integer> rightIndex) {
        if (leftIndex.size() != rightIndex.size()) {
            System.out.println("Tuple: Unknown comparision of the tuples");
            System.exit(1);
            return 0;
        }
        for (int i = 0; i < leftIndex.size(); ++i) {
            Object leftdata = left.dataAt(leftIndex.get(i));
            Object rightdata = right.dataAt(rightIndex.get(i));
            if (leftdata.equals(rightdata)) continue;
            if (leftdata instanceof Integer) {
                return ((Integer) leftdata).compareTo((Integer) rightdata);
            } else if (leftdata instanceof String) {
                return ((String) leftdata).compareTo((String) rightdata);
            } else if (leftdata instanceof Float) {
                return ((Float) leftdata).compareTo((Float) rightdata);
            } else {
                System.out.println("Tuple: Unknown comparision of the tuples");
                System.exit(1);
                return 0;
            }
        }
        return 0;
    }

    // Hashing the Tuples helps us to check for distinct values
    // Code from https://www.baeldung.com/java-hashcode
    @Override
    public int hashCode() {
        int hash = 7;
        for (Object elementdata : _data) {
            if (elementdata instanceof Integer) {
                hash = 31 * hash + (int) elementdata;
            } else if (elementdata instanceof String || elementdata instanceof Float) {
                hash = 31 * hash + elementdata.hashCode();
            } else {
                System.out.println("Unrecognised tuple type");
                System.exit(1);
                return 0;
            }
        }

        return hash;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (Object o: _data) {
            if (o instanceof Integer) {
                sb.append(Integer.toString((int) o));
            } else if (o instanceof String) {
                sb.append((String) o);
            } else if (o instanceof Float) {
                sb.append(Float.toString((Float) o));
            } else {
                System.out.println("Tuple toString() failed due to unsupported Tuple element type");
            }
            sb.append(",");
        }

        // Trim the last comma
        return sb.substring(0, sb.length() - 1);
    }
}
