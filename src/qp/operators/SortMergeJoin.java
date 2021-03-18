package qp.operators;

import java.io.*;
import java.util.ArrayList;

import qp.algorithms.ExternalSort;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    private int batchsize;

    private ArrayList<Integer> leftindex;
    private ArrayList<Integer> rightindex;

    private Batch leftBatch;
    private Batch rightBatch;

    private Tuple leftTuple = null;
    private Tuple rightTuple = null;

    private int leftCursor = 0;
    private int rightCursor = 0;

    // The right partition that is currently being joined in.
    private ArrayList<Tuple> rightPartition = new ArrayList<>();
    // The index of the tuple that is currently being processed in the current right partition (0-based).
    private int rightPartitionIndex = 0;
    // The next right tuple (i.e., the first element of the next right partition).
    private Tuple nextRightTuple = null;

    private boolean isEndedLeft = false;
    private boolean isEndedRight = false;

    static int filenum = 0;

    String filename;
    boolean isAsc;

    ObjectInputStream rightIns;
    ObjectInputStream leftIns;
    private boolean eos;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        eos = false;
    }


    @Override
    public boolean open() {
        // set batchsize, leftindex, right index
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;
        Batch leftpage;

        if (!right.open()) {
            return false;
        } else {
            //materialize right
            filenum++;
            filename = "SMJRightTemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("SortMerge: Error writing to temporary file");
                return false;
            }
        }
        if (!left.open()) {
            return false;
        } else {
            filenum++;
            filename = "SMJLeftTemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
                while ((leftpage = left.next()) != null) {
                    out.writeObject(leftpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("SortMerge: Error writing to temporary file");
                return false;
            }
        }

        ExternalSort externalsort = new ExternalSort(Batch.getPageSize(), numBuff);
        String sortedLeftFilePath = "";
        String sortedRightFilePath = "";

        // sort the left here
        try {
            sortedLeftFilePath = externalsort.sort(filename, tuplesize, leftindex, !this.isAsc);
        } catch (IOException ioe) {
            System.out.println("Failed to sort file during sort merge join");
            System.exit(1);
        }

        try {
            leftIns = new ObjectInputStream(new FileInputStream(sortedLeftFilePath));
        } catch (IOException ioe) {
            System.exit(1);
        }

        // sort the right here
        try {
            sortedRightFilePath = externalsort.sort(filename, tuplesize, rightindex, !this.isAsc);
        } catch (IOException ioe) {
            System.out.println("Failed to sort file during sort merge join");
            System.exit(1);
        }

        try {
            rightIns = new ObjectInputStream(new FileInputStream(sortedRightFilePath));
        } catch (IOException ioe) {
            System.exit(1);
        }

        return true;
    }


    @Override
    public Batch next() {
        if (isEndedLeft || isEndedRight) {
            close();
            return null;
        }

        // To handle the 1st run.
        if (leftBatch == null) {
            leftBatch = getNextBatch(leftIns);
            if (leftBatch == null) {
                isEndedLeft = true;
                return null;
            }
            leftTuple = getNextLeftTuple();
            if (leftTuple == null) {
                isEndedLeft = true;
                return null;
            }
        }
        if (rightBatch == null) {
            rightBatch = getNextBatch(rightIns);
            if (rightBatch == null) {
                isEndedRight = true;
                return null;
            }
            rightPartition = createNextRightPartition();
            if (rightPartition.isEmpty()) {
                isEndedRight = true;
                return null;
            }
            rightPartitionIndex = 0;
            rightTuple = rightPartition.get(rightPartitionIndex);
        }

        Batch outBatch = new Batch(batchsize);
        while (!outBatch.isFull()) {
            int diff = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
            if (diff == 0) {
                outBatch.add(leftTuple.joinWith(rightTuple));
                if (rightPartitionIndex < rightPartition.size() - 1) {
                    rightPartitionIndex++;
                    rightTuple = rightPartition.get(rightPartitionIndex);
                } else {
                    Tuple nextLeftTuple = getNextLeftTuple();
                    if (nextLeftTuple == null) {
                        isEndedLeft = true;
                        break;
                    }
                    diff = Tuple.compareTuples(leftTuple, nextLeftTuple, leftindex, leftindex);
                    leftTuple = nextLeftTuple;

                    if (diff == 0) {
                        rightPartitionIndex = 0;
                        rightTuple = rightPartition.get(0);
                    } else {
                        rightPartition = createNextRightPartition();
                        if (rightPartition.isEmpty()) {
                            isEndedRight = true;
                            break;
                        }
                        rightPartitionIndex = 0;
                        rightTuple = rightPartition.get(rightPartitionIndex);
                    }
                }
            } else if (diff < 0) {
                leftTuple = getNextLeftTuple();
                if (leftTuple == null) {
                    isEndedLeft = true;
                    break;
                }
            } else {
                rightPartition = createNextRightPartition();
                if (rightPartition.isEmpty()) {
                    isEndedRight = true;
                    break;
                }

                rightPartitionIndex = 0;
                rightTuple = rightPartition.get(rightPartitionIndex);
            }
        }

        return outBatch;
    }

    private ArrayList<Tuple> createNextRightPartition() {
        ArrayList<Tuple> partition = new ArrayList<>();
        int diff = 0;
        if (nextRightTuple == null) {
            nextRightTuple = getNextRightTuple();
            if (nextRightTuple == null) {
                return partition;
            }
        }

        while (diff == 0) {
            partition.add(nextRightTuple);

            nextRightTuple = getNextRightTuple();
            if (nextRightTuple == null) {
                break;
            }
            diff = Tuple.compareTuples(partition.get(0), nextRightTuple, rightindex, rightindex);
        }

        return partition;
    }


    private Tuple getNextLeftTuple() {
        if (leftBatch == null) {
            isEndedLeft = true;
            return null;
        } else if (leftCursor == leftBatch.size()) {
            leftBatch = getNextBatch(leftIns);
            leftCursor = 0;
        }

        if (leftBatch == null || leftBatch.size() <= leftCursor) {
            isEndedLeft = true;
            return null;
        }

        Tuple nextLeftTuple = leftBatch.get(leftCursor);
        leftCursor++;
        return nextLeftTuple;
    }

    private Tuple getNextRightTuple() {
        if (rightBatch == null) {
            return null;
        } else if (rightCursor == rightBatch.size()) {
            rightBatch = getNextBatch(rightIns);
            rightCursor = 0;
        }

        if (rightBatch == null || rightBatch.size() <= rightCursor) {
            return null;
        }

        Tuple next = rightBatch.get(rightCursor);
        rightCursor++;
        return next;
    }

    public Batch getNextBatch(ObjectInputStream is) {
        if (eos) {
            return null;
        }

        Batch outbatch = new Batch(batchsize);
        for (int i = 0; i < batchsize; i++) {
            try {
                Tuple t = ExternalSort.readTuple(is);
                outbatch.add(t);
            } catch (EOFException eoe) {
                eos = true;
                break;
            } catch (IOException ioe) {
                System.out.println("Cannot read from sorted file in order by");
                System.exit(1) ;
            }
        }

        return outbatch;
    }

    @Override
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }


}