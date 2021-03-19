/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    boolean isAggregate;         // Aggregation function involved

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    public boolean isAggregate() {
        return isAggregate;
    }

    /**
     * Projection must all be aggregation, or no aggregation. All or nothing behaviour.
     * @return if the projection is aggregation valid
     */
    private boolean isAggregationValid() {
        return attrset.stream().distinct().count() == 1;
    }

    private int compareObjects(Object o1, Object o2) {
        if (o1 instanceof Integer) {
            return ((Integer) o1).compareTo((Integer) o2);
        } else if (o1 instanceof String) {
            return ((String) o1).compareTo((String) o2);
        } else if (o1 instanceof Float) {
            return ((Float) o1).compareTo((Float) o2);
        } else {
            System.out.println("Object is of an unknown type");
            System.exit(1);
        }

        return 0;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                isAggregate = true;
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }

        if (isAggregate && !isAggregationValid()) {
            System.err.println("Invalid query: Either aggregate all attributes or none");
            System.exit(1);
        }

        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);

        if (isAggregate) {
            int count = 0;    // Risk of overflow here but we assume that this will not happen.
            ArrayList<Object> aggregates = null;

            while ((inbatch = base.next()) != null) {
                for (int i = 0; i < inbatch.size(); i++) {
                    count++;
                    Tuple basetuple = inbatch.get(i);

                    if (aggregates == null) {
                        aggregates = new ArrayList<>();
                        for (int j = 0; j < attrset.size(); j++) {
                            Object data = basetuple.dataAt(attrIndex[j]);
                            int attrType = attrset.get(j).getAggType();

                            if (attrType == Attribute.COUNT) {
                                data = count;
                            }

                            aggregates.add(data);
                        }
                        continue;
                    }

                    for (int j = 0; j < attrset.size(); j++) {
                        int attributeType = attrset.get(j).getAggType();
                        Object data = basetuple.dataAt(attrIndex[j]);

                        switch (attributeType) {
                        case Attribute.MAX:
                            if (compareObjects(data, aggregates.get(j)) > 0) {
                                aggregates.set(j, data);
                            }
                            break;
                        case Attribute.MIN:
                            if (compareObjects(data, aggregates.get(j)) < 0) {
                                aggregates.set(j, data);
                            }
                            break;
                        case Attribute.COUNT:
                            aggregates.set(j, count);
                            break;
                        case Attribute.SUM:
                            if (data instanceof Integer) {
                                aggregates.set(j, (Integer) aggregates.get(j) + (Integer) data);
                            } else if (data instanceof Float) {
                                aggregates.set(j, (Float) aggregates.get(j) + (Float) data);
                            } else if (data instanceof String){
                                aggregates.set(j, (String) aggregates.get(j) + (String) data);
                            } else {
                                System.exit(1);
                            }
                            break;
                        case Attribute.AVG:
                            if (data instanceof Integer) {
                                aggregates.set(j, (Integer) aggregates.get(j) + (Integer) data);
                            } else if (data instanceof Float) {
                                aggregates.set(j, (String) aggregates.get(j) + (String) data);
                            } else {
                                // Don't average strings because thats weird
                                System.err.println("How to average string?");
                                System.exit(-1);
                            }
                        }
                    }
                }
            }

            // If no results just return null
            if (aggregates == null) {
                return null;
            }

            // Type casting from Int to Float
            for (int i = 0; i < attrset.size(); i++) {
                if (attrset.get(i).getAggType() == Attribute.AVG) {
                    if (aggregates.get(i) instanceof Integer) {
                        aggregates.set(i, (Integer) aggregates.get(i) / (float) count);
                    } else if (aggregates.get(i) instanceof Integer) {
                        aggregates.set(i, (Float) aggregates.get(i) / count);
                    }
                }
            }

            outbatch.add(new Tuple(aggregates));
            return outbatch;
        } else {
            /** all the tuples in the inbuffer goes to the output buffer **/
            inbatch = base.next();

            if (inbatch == null) {
                return null;
            }

            for (int i = 0; i < inbatch.size(); i++) {
                Tuple basetuple = inbatch.get(i);
                //Debug.PPrint(basetuple);
                //System.out.println();
                ArrayList<Object> present = new ArrayList<>();
                for (int j = 0; j < attrset.size(); j++) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    present.add(data);
                }
                Tuple outtuple = new Tuple(present);
                outbatch.add(outtuple);
            }
            return outbatch;
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
