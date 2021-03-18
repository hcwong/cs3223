package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import qp.algorithms.ExternalSort;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class Orderby extends Operator {
    Operator base;
    ArrayList<Attribute> attrToSortBy;
    int batchsize;
    boolean isAsc;
    String filename;
    boolean eos;
    int numBuff;
    ObjectInputStream is;

    static int filenum = 0;

    public Orderby(Operator base, ArrayList<Attribute> as, boolean isAsc, int type, int numBuff) {
        super(type);
        this.base = base;
        this.attrToSortBy = as;
        this.isAsc = isAsc;
        this.eos = false;
        this.numBuff = numBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schm) {
        this.schema = schm;
    }

    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (base instanceof NestedJoin) {
            System.out.println("1");
        }
        if (!base.open()) return false;

        // We materalize the result and sort it
        filenum++;
        filename = "Orderbytemp-" + String.valueOf(filenum);
        Batch nextPage;

        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
            while ((nextPage = base.next()) != null) {
                for (Tuple t : nextPage.getTuples()) {
                    out.writeObject(t);
                }
            }
        } catch (IOException io) {
            System.out.println("IO Exception in order by");
            System.exit(1);
        }

        ArrayList<Attribute> schemaAttrs = schema.getAttList();
        ArrayList<Integer> indexes = new ArrayList<>();
        // TODO: Replace 10 with something else
        ExternalSort externalsort = new ExternalSort(Batch.getPageSize(), 10);
        String sortedFilePath = "";
        for (Attribute a : attrToSortBy) {
            indexes.add(schemaAttrs.indexOf(a));
        }
        try {
            sortedFilePath = externalsort.sort(filename, tuplesize, indexes, !this.isAsc);
        } catch (IOException ioe) {
            System.out.println("Failed to sort file during order by");
            System.exit(1);
        }

        try {
            is = new ObjectInputStream(new FileInputStream(sortedFilePath));
        } catch (IOException ioe) {
            System.exit(1);
        }

        return true;
    }

    public Batch next() {
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

    public boolean close() {
        File f = new File(filename);
        f.delete();
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrToSortBy.size(); i++)
            newattr.add((Attribute) attrToSortBy.get(i).clone());
        Orderby newOrderby = new Orderby(newbase, newattr, isAsc, optype, numBuff);
        newOrderby.setSchema((Schema) newbase.getSchema().clone());
        return newOrderby;
    }
}
