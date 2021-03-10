package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.BPlusTree;
import qp.utils.BPlusTreeKey;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class IndexNestedJoin extends Join {
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> innerindex;  // Indices of the join attributes in inner table
    ArrayList<Integer> outerindex;  // Indices of the join attributes in the outer table
    Batch outbatch;                 // Buffer page for output
    Batch outerBatch;                // Buffer page for left input stream

    int ocurs;                      // Cursor for left side buffer
    int icurs;                      // Cursor for right side buffer
    boolean eoso;                   // Whether end of stream (left table) is reached
    boolean eosi;                   // Whether end of stream (right table) is reached

    Operator outer;                 // Which operator is the inner or outer loop
    Operator inner;                 // Which operator is the inner or outer loop
    private int conditionExprType;  // Type of condition expr
    private BPlusTree<BPlusTreeKey, Long> index;   // Index
    private int conditionUsedForIndex;             // The index of the attribute used for joining

    public IndexNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     *  During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        setup();

        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        // Make left batch the size of the number of buffers available for join operator - 2
        // -2 because we do not count the buffer for the inner file and the output buffer
        outerBatch = new Batch(batchsize * (BufferManager.getBuffersPerJoin() - 2));

        /** find indices attributes of join conditions **/
        ArrayList<Integer> leftindex = new ArrayList<>();
        ArrayList<Integer> rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        if (left == inner) {
            innerindex = leftindex;
            outerindex = rightindex;
        } else {
            innerindex = rightindex;
            outerindex = leftindex;
        }

        /** initialize the cursors of input buffers **/
        ocurs = 0;
        icurs = 0;
        eoso = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosi = true;

        if (!right.open())
            return false;
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * Gets the next batch from the join operation
     * @return Batch
     */
    public Batch next() {
        int i, j;
        if (eoso) {
            return null;
        }
        outbatch = new Batch(batchsize);
        return indexJoin(outbatch);
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return true;
    }

    /**
     * This function performs the indexJoin in order to return an outbatch
     * @param outbatch
     * @return Batch
     */
    private Batch indexJoin(Batch outbatch) {
        // We iterate until the end of the outer file
        while (!eoso) {
            while (!outerBatch.isFull()) {
                Batch nextBatch = outer.next();
                if (nextBatch == null) {
                    eoso = true;
                    break;
                }
                outerBatch.addBatch(nextBatch);
            }

            // Need to use indexes here
            for (; ocurs < outerBatch.size(); ocurs++) {
                Tuple outerTuple = outerBatch.get(ocurs);
                ArrayList<Tuple> matchingTuples = getMatchOnEquality(outerTuple);

                // Matching tuple not found
                if (matchingTuples == null) {
                    continue;
                }

                for (Tuple innerTuple: matchingTuples) {
                    if (outerTuple.checkJoin(innerTuple, innerindex, outerindex)) {
                        if (inner == left) {
                            outbatch.add(innerTuple.joinWith(outerTuple));
                        } else
                            outbatch.add(outerTuple.joinWith(innerTuple));
                    }

                    if (outbatch.isFull()) {
                        return outbatch;
                    }
                }
            }

            if (eoso) {
                return outbatch;
            }

            // Clear the outerbatch because at this point we would have iterated through all the data
            outerBatch.clear();
            ocurs = 0;
        }

        return null;
    }

    private ArrayList<Tuple> getMatchOnEquality(Tuple outerTuple) {
        // Attribute used for sorting
        int outerTupleIndex = outerindex.get(innerindex.indexOf(conditionUsedForIndex));
        List<Object> keyValues = new ArrayList<>();
        keyValues.add(outerTuple.dataAt(outerTupleIndex));
        BPlusTreeKey key = new BPlusTreeKey(keyValues);
        Long offset = index.search(key);

        if (offset == null)
            return null;

        ArrayList<Tuple> innerTuplesToJoin = new ArrayList<>();

        try {
            // NOTE: Assumes that cwd is in same directory as table
            FileInputStream fins = new FileInputStream(
                inner.getSchema().getAttribute(outerTupleIndex).getTabName() + ".tbl");
            ObjectInputStream ins = new ObjectInputStream(fins);

            fins.getChannel().position(offset);

            // First find the first instance of the matching tuple
            Tuple innerTuple = (Tuple) ins.readObject();
            while (!innerTuple.dataAt(conditionUsedForIndex).equals(outerTuple.dataAt(outerTupleIndex))) {
                innerTuple = (Tuple) ins.readObject();
            }
            innerTuplesToJoin.add(innerTuple);
            innerTuple = (Tuple) ins.readObject();

            // Get the rest of the matching tuples
            while (innerTuple.dataAt(conditionUsedForIndex).equals(outerTuple.dataAt(outerTupleIndex))) {
                innerTuplesToJoin.add(innerTuple);
                innerTuple = (Tuple) ins.readObject();
            }

            ins.close();
            fins.close();

            return innerTuplesToJoin;
        } catch (EOFException eof) {
            return innerTuplesToJoin;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Failed to get the index file");
            System.exit(1);
        } catch (ClassNotFoundException ce) {
            System.exit(1);
        }

        return null;
    }

    /**
     * Setup the operator for an index nested join
     * We read in the index if it exists and check for a condition we can do the index nested join on
     */
    private void setup() {
        List<Attribute> leftConditions = new ArrayList<>();
        List<Attribute> rightConditions = new ArrayList<>();
        for (Condition cond: conditionList) {
            if (cond.getExprType() == Condition.EQUAL) {
                leftConditions.add(cond.getLhs());
                rightConditions.add((Attribute) cond.getRhs());
            }
        }

        // If no equality condition to sort by, use random inequality condition
        if (leftConditions.isEmpty()) {
            for (Condition cond: conditionList) {
                leftConditions.add(cond.getLhs());
                rightConditions.add((Attribute) cond.getRhs());
            }
        }

        HashMap<Attribute, String> leftMap = checkAttributes(leftConditions);
        HashMap<Attribute, String> rightMap = checkAttributes(rightConditions);
        String indexPath = "";

        // Set the inner and outer schema in the loop
        if (!leftMap.isEmpty() && left instanceof Scan) {
            inner = left;
            outer = right;
            Attribute indexAttr = (Attribute) leftMap.keySet().toArray()[0];
            indexPath = leftMap.get(indexAttr);
            for (Condition cond: conditionList) {
                if (cond.getLhs().equals(indexAttr)) {
                    conditionUsedForIndex = left.getSchema().indexOf(indexAttr);
                    break;
                }
            }
        } else if (!rightMap.isEmpty() && right instanceof Scan){
            inner = right;
            outer = left;
            Attribute indexAttr = (Attribute) rightMap.keySet().toArray()[0];
            indexPath = rightMap.get(indexAttr);
            for (Condition cond: conditionList) {
                if (cond.getRhs().equals(indexAttr)) {
                    conditionUsedForIndex = right.getSchema().indexOf(indexAttr);
                    break;
                }
            }
        } else {
            // TODO: Default to block nested loop join
            System.out.println("Implement block nested loop here");
            System.exit(1);
        }

        // Load the index into memory
        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(indexPath));
            index = (BPlusTree<BPlusTreeKey, Long>) ins.readObject();
        } catch (IOException ioe) {
            System.out.println("Cannot find index file in index nested join");
            System.exit(1);
        } catch (ClassNotFoundException ce) {
            System.out.println("Class not found in index nested join");
            System.exit(1);
        }
    }

    /**
     * This function checks if there exists a an index for a given table ordered by a certain key.
     * @param attr
     * @return empty string if no index is found, else returns an absolute path to the file.
     */
    private static String getIndexIfExists(Attribute attr) {
        String cwd = Paths.get("").toAbsolutePath().getParent().toString();
        File indexesDir = new File(cwd + "/indexes/");

        for (String filename: indexesDir.list()) {
            // Due to the way the BPlusTree key works, we can only match to the exact index
            if (filename.equals
                (String.format("%s-%s", attr.getTabName(), attr.getColName()))
            )
                return String.format("%s/indexes/%s-%s", cwd, attr.getTabName(), attr.getColName());
        }

        return "";
    }

    /**
     * Given a list of attributes, return those attributes for which an apt index file exists.
     * @param attrs
     * @return HashMap
     */
    private static HashMap<Attribute, String> checkAttributes(List<Attribute> attrs) {
        HashMap<Attribute, String> indexesMap = new HashMap<>();

        for (Attribute attr: attrs) {
            String result = getIndexIfExists(attr);
            if (!result.equals("")) {
                indexesMap.put(attr, result);
            }
        }

        return indexesMap;
    }
}
