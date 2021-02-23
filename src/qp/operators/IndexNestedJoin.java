package qp.operators;

import java.io.File;
import java.io.ObjectInputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;

public class IndexNestedJoin extends Join {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    Operator outer;                 // Which operator is the inner or outer loop
    Operator inner;                 // Which operator is the inner or outer loop
    private boolean isInequalityComp;
    private HashMap<Attribute, String> innerIndexMap;

    public IndexNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    private void setInnerOuter() {
        isInequalityComp = false;
        List<Attribute> leftConditions = new ArrayList<>();
        List<Attribute> rightConditions = new ArrayList<>();
        for (Condition cond: conditionList) {
            if (cond.getExprType() == Condition.EQUAL) {
                leftConditions.add(cond.getLhs());
                rightConditions.add((Attribute) cond.getRhs());
            }
        }

        if (leftConditions.isEmpty()) {
            isInequalityComp = true;
            for (Condition cond: conditionList) {
                leftConditions.add(cond.getLhs());
                rightConditions.add((Attribute) cond.getRhs());
            }
        }

        HashMap<Attribute, String> leftMap = checkAttributes(leftConditions);
        HashMap<Attribute, String> rightMap = checkAttributes(rightConditions);

        // Set the inner and outer schema in the loop
        if (rightMap.isEmpty() && leftMap.isEmpty()) {
            System.out.println("Can't index join with no indexes available");
            System.exit(1);
        } else if (leftMap.isEmpty()) {
            inner = right;
            outer = left;
            innerIndexMap = rightMap;
        } else {
            inner = left;
            outer = right;
            innerIndexMap = leftMap;
        }
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
}
