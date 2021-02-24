package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.BPlusTree;
import qp.utils.BPlusTreeKey;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class IndexNestedJoin extends Join {
    UUID uuid;                      // Get a unique file name
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> innerindex;  // Indices of the join attributes in inner table
    ArrayList<Integer> outerindex;  // Indices of the join attributes in the outer table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch outerBatch;                // Buffer page for left input stream
    Batch innerBatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int ocurs;                      // Cursor for left side buffer
    int icurs;                      // Cursor for right side buffer
    boolean eoso;                   // Whether end of stream (left table) is reached
    boolean eosi;                   // Whether end of stream (right table) is reached

    Operator outer;                 // Which operator is the inner or outer loop
    Operator inner;                 // Which operator is the inner or outer loop
    private int conditionExprType;  // Type of condition expr
    private BPlusTree<BPlusTreeKey, Long> index;   // Index
    private Condition conditionUsedForIndex;       // The condition which is used to search the index

    public IndexNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        uuid = UUID.randomUUID();
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

        Batch rightpage;
        /** initialize the cursors of input buffers **/
        ocurs = 0;
        icurs = 0;
        eoso = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosi = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            // TODO: Abstract this out into another function
            try {
                ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(getRfName()));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("IndexNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
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
        int i, j;
        if (eoso) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!eoso) {
            if (ocurs == 0 && eosi) {
                // Fetch all the outer buffers one can possibly fetch
                while (!outerBatch.isFull()) {
                    Batch nextBatch = outer.next();
                    if (nextBatch == null) {
                        eoso = true;
                        break;
                    }
                    outerBatch.addBatch(nextBatch);
                }

//                if (outerBatch == null) {
//                    eoso = true;
//                    return outbatch;
//                }

                /** Whenever a new left page came, we have to start the
                 ** scanning of right table
                 **/
//                try {
//                    in = new ObjectInputStream(new FileInputStream(rfname));
//                    eosi = false;
//                } catch (IOException io) {
//                    System.err.println("NestedJoin:error in reading the file");
//                    System.exit(1);
//                }

            }
            // TODO: Depending on the condition expr type, we use a different function
            // Need to write 3 functions
            // The first is the index nested join
            // The second is the special case if it is a range query (might be able to merge)
            // The third is just a generic block nested join

            // For index nested join, we must load the pages one by one from the
        }
        return outbatch;
    }

    /**
     * Setup the operator for an index nexted join
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

        // If no equality condition to sort by
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
        if (rightMap.isEmpty() && leftMap.isEmpty()) {
            System.out.println("Can't index join with no indexes available");
            System.exit(1);
        } else if (leftMap.isEmpty()) {
            inner = right;
            outer = left;
            Attribute indexAttr = (Attribute) rightMap.keySet().toArray()[0];
            indexPath = rightMap.get(indexAttr);
            for (Condition cond: conditionList) {
                if (cond.getRhs().equals(indexAttr)) {
                    conditionUsedForIndex = cond;
                    break;
                }
            }
        } else {
            inner = left;
            outer = right;
            Attribute indexAttr = (Attribute) leftMap.keySet().toArray()[0];
            indexPath = leftMap.get(indexAttr);
            for (Condition cond: conditionList) {
                if (cond.getLhs().equals(indexAttr)) {
                    conditionUsedForIndex = cond;
                    break;
                }
            }
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

        // Make left batch the size of the number of buffers available for join operator - 2
        // -2 because we do not count the buffer for the inner file and the output buffer
        outerBatch = new Batch(batchsize * (BufferManager.getBuffersPerJoin() - 2));
    }

    /**
     * This function checks if there exists a an index for a given table ordered by a certain key.
     * @param attr
     * @return empty string if no index is found, else returns an absolute path to the file.
     */
    private static String getIndexIfExists(Attribute attr) {
        String cwd = Paths.get("").toAbsolutePath().toString();
        File indexesDir = new File(cwd + "/indexes");

        for (String filename: indexesDir.list()) {
            if (filename.startsWith
                (String.format("%s-%s", attr.getTabName(), attr.getColName()))
            )
                return filename;
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
     * @return absolute file to Rf
     */
    private String getRfName() {
        return Paths.get("").toAbsolutePath().toString()
            + "/tmp/right-" +  uuid.toString();
    }
}
