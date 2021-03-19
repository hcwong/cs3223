package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashDistinct extends Operator {

    Operator base;
    String filename;
    int batchSize;

    public HashDistinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    private int hash(Tuple tuple, int numBuckets) {
        return tuple.hashCode() % numBuckets;
    }

    public boolean open() {
        if (!base.open()) {
            return false;
        }

        int numBuffer = BufferManager.getNumBuffer() - 1;
        ArrayList<Batch> buffers = new ArrayList<>(numBuffer);
        Batch batch = base.next();
        batchSize = batch.capacity();
        for (int i = 0; i < numBuffer; i++) {
            buffers.add(new Batch(batchSize));
        }

        int j;
        Tuple t;
        Batch b;
        TupleWriter tw;

        ArrayList<TupleWriter> tupleWriters = IntStream.range(0, numBuffer)
                .mapToObj(i -> new TupleWriter(String.format("temp-%d.tbl", i), batchSize))
                .collect(Collectors.toCollection(ArrayList::new));
        tupleWriters.forEach(TupleWriter::open);
        while (batch != null) {
            for (int i = 0; i < batch.size(); i++) {
                t = batch.get(i);
                j = hash(t, numBuffer);
                b = buffers.get(j);
                if (!b.contains(t)) {
                    b.add(t);
                }
                if (b.isFull()) {
                    tw = tupleWriters.get(j);
                    tw.open();
                    b.getTuples().forEach(tw::next);
                    tw.close();
                    b.clear();
                }
            }
            batch = base.next();
        }
        for (int i = 0; i < batchSize; i++) {
            b = buffers.get(i);
            tw = tupleWriters.get(i);
            tw.open();
            b.getTuples().forEach(tw::next);
            tw.close();
        }
        tupleWriters.forEach(TupleWriter::close);

        int numBuckets = (numBuffer / 2) | 1;
        Hashtable<Integer, Tuple> hashTable = new Hashtable<>();
        filename = "temp-distinct.tbl";
        tw = new TupleWriter(filename, batchSize);
        for (int i = 0; i < numBuffer; i++) {
            TupleReader tr = new TupleReader(String.format("temp-%d.tbl", i), batchSize);
            tr.open();
            while (!tr.isEOF()) {
                t = tr.next();
                j = hash(t, numBuckets);
                hashTable.put(j, t);
            }
            tr.close();
            hashTable.values().forEach(tw::next);
            hashTable.clear();
        }
        tw.close();
        return true;
    }

    public Batch next() {
        Batch outBatch = new Batch(batchSize);
        TupleReader tupleReader = new TupleReader(filename, batchSize);
        tupleReader.open();
        while (!tupleReader.isEOF()) {
            Tuple t = tupleReader.next();
            outBatch.add(t);
        }
        tupleReader.close();
        return outBatch;
    }

    public boolean close() {
        File file = new File(filename);
        file.delete();
        return base.close();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }
}
