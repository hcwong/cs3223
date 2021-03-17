package qp.operators;

import java.util.ArrayList;

import qp.algorithms.ExternalSort;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

public class SortMergeJoin extends Join {
    private int batchSize;

    private int leftIndex;
    private int rightIndex;

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

    // Type of the join attribute.
    private int attrType;


    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    @Override
    public boolean open() {
        // Sorts the left and right relation.
        left.open();
        right.open();

        /** select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        Attribute leftAttr = getCondition().getLhs();
        Attribute rightAttr = (Attribute) getCondition().getRhs();
        leftIndex = left.getSchema().indexOf(leftAttr);
        rightIndex = right.getSchema().indexOf(rightAttr);

        attrType = left.getSchema().typeOf(leftAttr);

        return super.open();
    }


    @Override
    public Batch next() {
        if (isEndedLeft || isEndedRight) {
            close();
            return null;
        }

        // To handle the 1st run.
        if (leftBatch == null) {
            leftBatch = left.next();
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
            rightBatch = right.next();
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

        Batch outBatch = new Batch(batchSize);
        while (!outBatch.isFull()) {
            int diff = Tuple.compareTuples(leftTuple, rightTuple, leftIndex, rightIndex);
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
                    diff = Tuple.compareTuples(leftTuple, nextLeftTuple, leftIndex, leftIndex);
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
            diff = Tuple.compareTuples(partition.get(0), nextRightTuple, rightIndex, rightIndex);
        }

        return partition;
    }


    private Tuple getNextLeftTuple() {
        if (leftBatch == null) {
            isEndedLeft = true;
            return null;
        } else if (leftCursor == leftBatch.size()) {
            leftBatch = left.next();
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
            rightBatch = right.next();
            rightCursor = 0;
        }

        if (rightBatch == null || rightBatch.size() <= rightCursor) {
            return null;
        }

        Tuple next = rightBatch.get(rightCursor);
        rightCursor++;
        return next;
    }

    @Override
    public boolean close() {
        left.close();
        right.close();
        return super.close();
    }
}
