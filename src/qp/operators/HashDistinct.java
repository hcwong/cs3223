package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleWriter;

import java.util.ArrayList;

public class HashDistinct extends Operator {

    Operator base;

    public HashDistinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    private int hash(Tuple tuple, int numBuffer) {
        return tuple.hashCode() % numBuffer;
    }

    public boolean open() {
        return base.open();
    }

    public Batch next() {
        int numBuffer = BufferManager.getNumBuffer() - 1;
        ArrayList<Batch> buffers = new ArrayList<>(numBuffer);
        Batch batch = base.next();
        int batchSize = batch.capacity();
        for (int i = 0; i < numBuffer; i++) {
            buffers.add(new Batch(batchSize));
        }

        int j;
        Tuple t;
        Batch b;

        TupleWriter tw = new TupleWriter("temp.tbl", batchSize);
        tw.open();
        while (batch != null) {
            for (int i = 0; i < batch.size(); i++) {
                t = batch.get(i);
                j = hash(t, numBuffer);
                b = buffers.get(j);
                if (!b.contains(t)) {
                    b.add(t);
                }
                if (b.isFull()) {
                    b.getTuples().forEach(tw::next);
                    b.clear();
                }
            }
            batch = base.next();
        }
        for (Batch bat : buffers) {
            bat.getTuples().forEach(tw::next);
        }
        tw.close();
        return null;
    }

    public boolean close() {
        return base.close();
    }
}
