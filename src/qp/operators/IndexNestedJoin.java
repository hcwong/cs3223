package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import qp.algorithms.ExternalSort;
import qp.utils.Attribute;
import qp.utils.BPlusTree;
import qp.utils.BPlusTreeKey;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class IndexNestedJoin extends Join {
    /**
     * The default ordering is left == outer and right == inner if it
     * defaults to a block nested join
     */
    int batchsize;                  // Number of tuples per out batch
    String rfname;
    ArrayList<Integer> innerindex;  // Indices of the join attributes in inner table
    ArrayList<Integer> outerindex;  // Indices of the join attributes in the outer table
    Batch outbatch;                 // Buffer page for output
    Batch outerBatch;                // Buffer page for left input stream

    int ocurs;                      // Cursor for left side buffer
    int icurs;                      // Cursor for right side buffer
    int matchingTuplesIndex;        // Enables us to handle a many to one join.
    boolean eoso;                   // Whether end of stream (outer table) is reached
    boolean eosi;                   // Whether end of stream (inner table) is reached
    ObjectInputStream rightInputStream;  // Used if it falls back to block nested join
    Batch rightBatch;               // Store the right batch's result

    Operator outer;                 // Which operator is the inner or outer loop
    Operator inner;                 // Which operator is the inner or outer loop
    private Condition conditionUsedForIndexJoin;  // Type of condition expr
    private BPlusTree<BPlusTreeKey, String> index;   // Index
    private int attrIndexInTreeIndex;             // The index of the attribute used for joining

    static int filenum = 0;         // Unique filename

    public IndexNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        this.conditionUsedForIndexJoin = null;
        this.rfname = null;
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
        outerBatch = new Batch(batchsize * (numBuff - 2));

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

        matchingTuplesIndex = 0;
        rightBatch = new Batch(0);

        if (!right.open()) {
            return false;
        } else {
            materializeRf();
        }
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
        // If we can do an index nested join, we do it
        if (conditionUsedForIndexJoin != null) {
            // We must check if ocurs is >= the outerbatch size
            // This is because it is possible for there to still be data in outerbatch while eoso is true.
            if (eoso && ocurs >= outerBatch.size()) {
                return null;
            }
            outbatch = new Batch(batchsize);
            return indexJoin(outbatch);
        } else {
            return blockNestedJoin();
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        if (conditionUsedForIndexJoin == null) {
            File f = new File(rfname);
            f.delete();
        }
        return true;
    }

    /**
     * This function performs the indexJoin in order to return an outbatch
     * @param outbatch
     * @return Batch
     */
    private Batch indexJoin(Batch outbatch) {
        // We iterate until the end of the outer file
        while (!eoso || ocurs < outerBatch.size()) {
            while (!outerBatch.isFull()) {
                Batch nextBatch = outer.next();
                if (nextBatch == null) {
                    eoso = true;
                    break;
                }
                outerBatch.addBatch(nextBatch);
            }

            // Need to use indexes here
            while (ocurs < outerBatch.size()) {
                Tuple outerTuple = outerBatch.get(ocurs);
                ArrayList<Tuple> matchingTuples = null;

                if (conditionUsedForIndexJoin.getExprType() == Condition.EQUAL) {
                    matchingTuples = getMatchOnEquality(outerTuple);
                } else {
                    matchingTuples = getMatchOnInequality(outerTuple);
                }

                // Matching tuple not found
                if (matchingTuples == null) {
                    continue;
                }

                while (matchingTuplesIndex < matchingTuples.size()) {
                    Tuple innerTuple = matchingTuples.get(matchingTuplesIndex);
                    matchingTuplesIndex++;

                    if (inner == left) {
                        if (innerTuple.checkJoin(outerTuple, innerindex, outerindex, conditionList)) {
                            outbatch.add(innerTuple.joinWith(outerTuple));
                        }
                    } else {
                        if (outerTuple.checkJoin(innerTuple, outerindex, innerindex, conditionList)) {
                            outbatch.add(outerTuple.joinWith(innerTuple));
                        }
                    }

                    if (outbatch.isFull()) {
                        // This is necessary so we dont repeat the ocurs the next time
                        // the for loop is called again
                        return outbatch;
                    }
                }
                matchingTuplesIndex = 0;
                // ocurs can only be incremented here
                ocurs++;
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
        int outerTupleIndex = outerindex.get(innerindex.indexOf(attrIndexInTreeIndex));
        List<Object> keyValues = new ArrayList<>();
        keyValues.add(outerTuple.dataAt(outerTupleIndex));
        BPlusTreeKey key = new BPlusTreeKey(keyValues);
        String batchPath = index.search(key);

        if (batchPath == null)
            return null;

        ArrayList<Tuple> innerTuplesToJoin = new ArrayList<>();
        String[] paths = batchPath.split("-");

        boolean continueReading = true;
        String basePath = String.join("-",
            Arrays.copyOfRange(paths, 0, paths.length - 1));
        int pathCount = Integer.parseInt(paths[paths.length - 1]);

        // We start reading from the
        while (continueReading) {
            FileInputStream fins = null;
            ObjectInputStream ins = null;
            try {
                fins = new FileInputStream
                    (basePath + "-" + Integer.toString(pathCount));
                ins = new ObjectInputStream(fins);
            } catch (FileNotFoundException fe) {
                return innerTuplesToJoin;
            } catch (IOException ioe) {
                ioe.printStackTrace();
                System.out.println("Failed to open batch file");
                System.exit(1);
            }

            boolean foundMatching = false;
            // Read all matching tuples from the current batchfile
            while (true) {
                try {
                    Tuple innerTuple = (Tuple) ins.readObject();
                    if (innerTuple.dataAt(attrIndexInTreeIndex)
                        .equals(outerTuple.dataAt(outerTupleIndex)))
                    {
                        foundMatching = true;
                        innerTuplesToJoin.add(innerTuple);
                    } else if (foundMatching) {
                        continueReading = false;
                        break;
                    }
                } catch (EOFException eof) {
                    pathCount++;
                    break;
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                    System.out.println("Issue while handling joining");
                    System.exit(1);
                } catch (ClassNotFoundException ce) {
                    System.exit(1);
                }
            }
        }

        return innerTuplesToJoin;
    }

    // TODO: Refactor
    private ArrayList<Tuple> getMatchOnInequality(Tuple outerTuple) {
        int outerTupleIndex = outerindex.get(innerindex.indexOf(attrIndexInTreeIndex));
        List<Object> keyValues = new ArrayList<>();
        keyValues.add(outerTuple.dataAt(outerTupleIndex));
        BPlusTreeKey key = new BPlusTreeKey(keyValues);

        List<String> batchPaths = new ArrayList<>();
        // TODO: This is very confusing and needs to be factored
        if (conditionUsedForIndexJoin.getExprType() == Condition.LESSTHAN) {
            if (inner == left) {
                batchPaths = index.searchRange(index.firstLeafKey, BPlusTree.RangePolicy.INCLUSIVE,
                    key, BPlusTree.RangePolicy.EXCLUSIVE);
            } else {
                batchPaths = index.searchRange(key, BPlusTree.RangePolicy.EXCLUSIVE,
                    index.lastLeafKey, BPlusTree.RangePolicy.INCLUSIVE);
            }
        } else if (conditionUsedForIndexJoin.getExprType() == Condition.GREATERTHAN) {
            if (inner == left) {
                batchPaths = index.searchRange(key, BPlusTree.RangePolicy.EXCLUSIVE,
                    index.lastLeafKey, BPlusTree.RangePolicy.INCLUSIVE);
            } else {
                batchPaths = index.searchRange(index.firstLeafKey, BPlusTree.RangePolicy.INCLUSIVE,
                    key, BPlusTree.RangePolicy.EXCLUSIVE);
            }
        } else if (conditionUsedForIndexJoin.getExprType() == Condition.LTOE) {
            if (inner == left) {
                batchPaths = index.searchRange(index.firstLeafKey, BPlusTree.RangePolicy.INCLUSIVE,
                    key, BPlusTree.RangePolicy.INCLUSIVE);
            } else {
                batchPaths = index.searchRange(key, BPlusTree.RangePolicy.INCLUSIVE,
                    index.lastLeafKey, BPlusTree.RangePolicy.INCLUSIVE);
            }
        } else if (conditionUsedForIndexJoin.getExprType() == Condition.GTOE) {
            if (inner == left) {
                batchPaths = index.searchRange(key, BPlusTree.RangePolicy.INCLUSIVE,
                    index.lastLeafKey, BPlusTree.RangePolicy.INCLUSIVE);
            } else {
                batchPaths = index.searchRange(index.firstLeafKey, BPlusTree.RangePolicy.INCLUSIVE,
                    key, BPlusTree.RangePolicy.INCLUSIVE);
            }
        } else {
            System.out.println("Condition type not recognised");
            System.exit(1);
        }

        // Make sure batchPaths is unique
        batchPaths = batchPaths.stream().distinct().collect(Collectors.toList());
        // Read the matching tuples into the result list
        ArrayList<Tuple> tuplesList = new ArrayList<>();
        for (String path: batchPaths) {
            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(new FileInputStream(path));
            } catch (IOException ioe) {
                System.out.println("Cannot find file at path");
                System.exit(1);
            }

            // We need to refigure out which is left and right because of the condition check
            Tuple leftTuple = null;
            Tuple rightTuple = null;
            int leftIndex = 0;
            int rightIndex = 0;

            if (left == inner) {
                rightTuple = outerTuple;
                rightIndex = outerTupleIndex;
            } else {
                leftTuple = outerTuple;
                leftIndex = outerTupleIndex;
            }

            while (true) {
                try {
                    Tuple innerTuple = ExternalSort.readTuple(ois);

                    if (left == inner) {
                        leftTuple = innerTuple;
                        leftIndex = attrIndexInTreeIndex;
                    } else {
                        rightTuple = innerTuple;
                        rightIndex = attrIndexInTreeIndex;
                    }

                   // Check if it fulfills the conditions
                   switch (conditionUsedForIndexJoin.getExprType()) {
                   case Condition.LESSTHAN:
                       if (Tuple.compareTuples(
                           leftTuple, rightTuple, leftIndex, rightIndex) < 0)
                       {
                           tuplesList.add(innerTuple);
                       }
                       break;
                   case Condition.GREATERTHAN:
                       if (Tuple.compareTuples(
                           leftTuple, rightTuple, leftIndex, rightIndex) > 0)
                       {
                           tuplesList.add(innerTuple);
                       }
                       break;
                   case Condition.LTOE:
                       if (Tuple.compareTuples(
                           leftTuple, rightTuple, leftIndex, rightIndex) <= 0)
                       {
                           tuplesList.add(innerTuple);
                       }
                       break;
                   case Condition.GTOE:
                       if (Tuple.compareTuples(
                           leftTuple, rightTuple, leftIndex, rightIndex) >= 0)
                       {
                           tuplesList.add(innerTuple);
                       }
                       break;
                   default:
                       System.out.println("Unable to get match on this condition expr type");
                       System.exit(1);
                   }
               } catch (EOFException eof) {
                   break;
               } catch (IOException ioe) {
                   System.exit(1);
               }
            }
        }

        return tuplesList;
    }

    /**
     * This is just a standard block nested join
     * The block nested loop works as such:
     * First load in as many left pages as possible
     * Then load in 1 right page and join all the tuples
     * Continue until the right side EOFs, then we reload all the leftpages again.
     * @return Next Batch to return
     */
    private Batch blockNestedJoin() {
        if (eoso && eosi) {
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!eoso || !eosi) {
            while (!eoso && !outerBatch.isFull()) {
                Batch nextBatch = outer.next();
                if (nextBatch == null) {
                    eoso = true;
                    break;
                }
                outerBatch.addBatch(nextBatch);
            }

            // If end of stream for right, restart it
            if (eosi) {
                try {
                    rightInputStream = new ObjectInputStream(new FileInputStream(rfname));
                    eosi = false;
                } catch (IOException ioe) {
                    System.out.println("IndexNestedJoin: Failed to open rf file");
                    System.exit(1);
                }
            }

            // Read in a new right batch if necessary
            if (icurs >= rightBatch.size()) {
                try {
                    rightBatch = (Batch) rightInputStream.readObject();
                    icurs = 0;
                } catch (EOFException eof) {
                    // If eosi is true, then we need to start reading in the new left blocks
                    eosi = true;
                    outerBatch.clear();
                } catch (ClassNotFoundException ce) {
                    System.exit(1);
                } catch (IOException ioe) {
                    System.out.println("Index Nested Join: Cannot read in right batch");
                    System.exit(1);
                }
            }

            while (icurs < rightBatch.size()) {
                Tuple rightTuple = rightBatch.get(icurs);

                while (ocurs < outerBatch.size()) {
                    Tuple leftTuple = outerBatch.get(ocurs);
                    ocurs++;

                    if (leftTuple.checkJoin(rightTuple, outerindex, innerindex, conditionList)) {
                        outbatch.add(leftTuple.joinWith(rightTuple));

                        if (outbatch.isFull()) {
                            return outbatch;
                        }
                    }
                }

                ocurs = 0;
                icurs++;
            }
        }

        // There may be a scenario where outbatch has some stuff remaining here
        return outbatch;
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
                    attrIndexInTreeIndex = left.getSchema().indexOf(indexAttr);
                    conditionUsedForIndexJoin = cond;
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
                    attrIndexInTreeIndex = right.getSchema().indexOf(indexAttr);
                    conditionUsedForIndexJoin = cond;
                    break;
                }
            }
        } else {
            outer = left;
            inner = right;
        }

        // Load the index into memory if using index nested loop
        if (conditionUsedForIndexJoin != null) {
            try {
                ObjectInputStream ins = new ObjectInputStream(new FileInputStream(indexPath));
                index = (BPlusTree<BPlusTreeKey, String>) ins.readObject();
            } catch (IOException ioe) {
                System.out.println("Cannot find index file in index nested join");
                System.exit(1);
            } catch (ClassNotFoundException ce) {
                System.out.println("Class not found in index nested join");
                System.exit(1);
            }
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

    /**
     * Fall back to block nested join if there are no indexes
     */
    private void materializeRf() {
        filenum++;
        rfname = "INJ-" + filenum;
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
            Batch rightpage = right.next();
            while (rightpage != null) {
                out.writeObject(rightpage);
                rightpage = right.next();
            }
            out.close();
        } catch (IOException io) {
            System.out.println("Index Nested Join: Error materializing right file");
            System.exit(1);
        }
    }
}
