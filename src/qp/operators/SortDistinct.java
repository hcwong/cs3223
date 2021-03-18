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
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class SortDistinct extends Operator {
    Operator base;
    int batchsize;
    String filename;
    boolean eos;
    int numBuff;
    ObjectInputStream is;

    static int filenum = 0;

    public SortDistinct(Operator base,  int type, int numBuff) {
        super(type);
        this.base = base;
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

        if (!base.open()) return false;

        // We materalize the result and sort it
        filenum++;
        filename = "SortDistincttemp-" + String.valueOf(filenum);
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

        ArrayList<Integer> indexes = new ArrayList<>();
        ExternalSort externalsort = new ExternalSort(Batch.getPageSize(), numBuff);
        String sortedFilePath = "";
        // Just sort it on some random attribute
        indexes.add(0);
        try {
            sortedFilePath = externalsort.sort(filename, tuplesize, indexes, false);
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

        Integer hashOfLastTuple = null;
        while (!outbatch.isFull() && !eos) {
            try {
                Tuple t = ExternalSort.readTuple(is);
                int hash = t.hashCode();
                // Because it is a sorted file, all identical tuples will be adjacent to one another
                // We just discard a tuple if it has the same hashcode as its immediate preceeding
                // tuples.
                if (hashOfLastTuple == null || hashOfLastTuple != hash) {
                    hashOfLastTuple = hash;
                    outbatch.add(t);
                }
            } catch (EOFException eoe) {
                eos = true;
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
        SortDistinct newSortDistinct = new SortDistinct(newbase, optype, numBuff);
        newSortDistinct.setSchema((Schema) newbase.getSchema().clone());
        return newSortDistinct;
    }
}
