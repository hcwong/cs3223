package qp.algorithms;

import java.io.*;
import java.nio.*;
import java.util.Collections;
import java.util.UUID;

import qp.utils.*;

/**
 * This class handles the external sort-merge algorithm.
 * The algorithm is as follows:
 * First read in the tuple batch by batch and partition them into separate files
 * Then merge them into a result file.
 */
public class ExternalSort {

    public UUID id; // Identifies the sort-id so we don't get confused between the different files on disk.
    public int pageSize;
    public int numberOfBuffers;
    public int tupleSize;


    public ExternalSort(int pageSize, int numberOfBuffers) {
        this.pageSize = pageSize;
        this.numberOfBuffers = numberOfBuffers
        this.id = UUID.randomUUID();
    }

    public String sort(String tblpath, String mdpath, int sortIndex) {
        Schema schema = null;

        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(mdpath));
            schema = (Schema) ins.readObject();
        } catch (ClassNotFoundException ce) {
            System.out.println("class not found exception --- error in schema object file");
            System.exit(1);
        }

        tupleSize = schema.getTupleSize();
        int batchSize = pageSize / tupleSize;
        ObjectInputStream tableIns = null;

        // First Step: Partition files
        try {
            tableIns = new ObjectInputStream(new FileInputStream(tblpath));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
            return false;
        }

        boolean eos = false;
        int initialRunSize = batchSize * numberOfBuffers;
        int initialRunCount = 0;
        String currentAbsPath = Paths.get("").toAbsolutePath().toString();
        while (!eos) {
            List<Tuple> outbatchTuplesList = new ArrayList<>();

            // While the batch is not full and eos is not reached, write to outfile
            while (!eos && outbatchTuplesList.size() != initialRunSize) {
                try {
                   Tuple data = (Tuple) tableIns.readObject();
                   outbatchTuplesList.add(data);
                } catch (EOFException eof) {
                    eos = true;
                } catch (IO Exception ioe) {
                    System.err.println("scan:error reading " + tblname);
                    System.exit(1);
                }
            }

            // If there are tuples in the list, sort and write it to disk
            if (!outbatchTuplesList.isEmpty()) {
                Collections.sort(outbatchTuplesList, new TupleComparator(sortIndex));
                ObjectOutputStream outs = new ObjectOutputStream(new FileOutputStream(
                                            currentAbsPath + "/tmp/" + this.id.toString() + "-0-" + initialRunCount.toString()));
                for (Tuple t: outbatchTuplesList)
                    outs.writeObject(t);

                initialRunCount++;
            }

        }

        return merge();
    }

    public String merge(int initialRunCount) {
        return "";
    }

    public boolean close(ObjectInputStream in) {
        try {
            in.close();
        } catch (IOException e) {
            System.err.println("Scan: Error closing " + filename);
            return false;
        }
        return true;
    }
}
