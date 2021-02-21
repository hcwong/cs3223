package qp.algorithms;

import java.io.*;
import java.nio.file.Paths;
import java.lang.Math;
import java.util.*;

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
    public String currentAbsPath;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("usage: java qp.algorithms.ExternalSort <tblpath> <mdpath> <sortindex>");
        }

        // Testing: Do not use call anything from External Sort unless you want to manual test it
        ExternalSort sort = new ExternalSort(100, 10);
        List<Integer> indexes = new ArrayList<>();
        for (int i = 2; i < args.length; i++)
            indexes.add(Integer.parseInt(args[i]));

        try {
            String resultFilePath = sort.sort(args[0], args[1], indexes);
            System.out.println(resultFilePath);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            System.out.println("Caught an IO Exception when attempting external merge sort");
        }
    }

    public ExternalSort(int pageSize, int numberOfBuffers) {
        this.pageSize = pageSize;
        this.numberOfBuffers = numberOfBuffers;
        this.id = UUID.randomUUID();
        this.currentAbsPath = Paths.get("").toAbsolutePath().toString();
    }

    /**
     * Returns a string which represents absolute path to merged file.
     * @param tblpath String
     * @param mdpath String
     * @param indexes List<Integer>
     * @return String
     * @throws IOException
     */
    public String sort(String tblpath, String mdpath, List<Integer> indexes)
        throws IOException {
        Schema schema = null;

        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(mdpath));
            schema = (Schema) ins.readObject();
        } catch (ClassNotFoundException ce) {
            System.out.println("class not found exception --- error in schema object file");
            System.exit(1);
        }

        tupleSize = schema.getTupleSize();
        int batchSize = (int) Math.floor(pageSize / tupleSize);

        if (pageSize < tupleSize) {
            System.out.println("Page size smaller than tuple size. Error");
            System.exit(1);
        }

        ObjectInputStream tableIns = null;

        // First Step: Partition files
        try {
            tableIns = new ObjectInputStream(new FileInputStream(tblpath));
        } catch (Exception e) {
            System.err.println(" Error reading file during sort");
            return "";
        }

        boolean eos = false;
        int initialRunSize = batchSize * numberOfBuffers;
        int initialRunCount = 0;
        while (!eos) {
            List<Tuple> outbatchTuplesList = new ArrayList<>();

            // While the batch is not full and eos is not reached, write to outfile
            while (!eos && outbatchTuplesList.size() != initialRunSize) {
                try {
                   Tuple data = readTuple(tableIns);
                   outbatchTuplesList.add(data);
                } catch (EOFException eof) {
                    eos = true;
                }
            }

            // If there are tuples in the list, sort and write it to disk
            if (!outbatchTuplesList.isEmpty()) {
                Collections.sort(outbatchTuplesList, new TupleComparator(indexes));
                ObjectOutputStream outs =
                    new ObjectOutputStream(new FileOutputStream(
                                            currentAbsPath + "/tmp/" + this.id.toString()
                                            + "-0-" + initialRunCount + ".tbl"));
                for (Tuple t: outbatchTuplesList)
                    outs.writeObject(t);

                initialRunCount++;
                outs.close();
            }
        }

        tableIns.close();
        if (initialRunCount == 1)
            return currentAbsPath + "/tmp/" + this.id.toString() + "-0-0.tbl";

        return merge(initialRunCount, indexes);
    }

    /**
     * Returns a string which represents the absolute path to the merged file.
     * @param initialRunCount int
     * @param indexes
     * @return String
     * @throws IOException
     */
    public String merge(int initialRunCount, List<Integer> indexes) throws IOException {
        int runCount = initialRunCount;
        int buffersForRuns = numberOfBuffers - 1;
        int runId = 1;
        int nextRunCount = 0;

        // The outer loop runs until we only have 1 run left - ie the sorted and merged result/final run.
        while (runCount > 1) {
            // This is for every pass of the sort-merge loop
            while (runCount > 0) {
                List<ObjectInputStream> inputStreams = new ArrayList<>(numberOfBuffers);
                List<Boolean> inputStreamsEof = new ArrayList<>(numberOfBuffers);
                // Using a priority queue will help reduce the k-way merge runtime
                PriorityQueue<Tuple> pq =
                    new PriorityQueue<>(buffersForRuns, new TupleComparator(indexes));
                HashMap<Tuple, Integer> tupleMap = new HashMap<>();
                ObjectOutputStream outs =
                    new ObjectOutputStream(new FileOutputStream(
                        String.format("%s/tmp/%s-%d-%d.tbl", currentAbsPath,
                            this.id.toString(), runId, nextRunCount)
                    ));

                // Open all the input streams to the previous runs
                for (int i = 0; i < buffersForRuns && runCount > 0; i++, runCount--) {
                    // We need to add nextRunCount * 10, else we will always be reading the
                    // first 10 runs of every pass
                    ObjectInputStream runInput = new ObjectInputStream(new FileInputStream(
                        String.format("%s/tmp/%s-%d-%d.tbl", currentAbsPath,
                            this.id.toString(), runId - 1, i + (nextRunCount * buffersForRuns))));
                    inputStreams.add(runInput);
                    inputStreamsEof.add(false);

                    try {
                        Tuple data = readTuple(runInput);
                        pq.add(data);
                        tupleMap.put(data, i);
                    } catch (EOFException eof) {
                        inputStreamsEof.set(i, true);
                    }
                }

                // Now we merge all the results in
                while (!(inputStreamsEof.stream().reduce(true, (a, b) -> a && b)) &&
                    !pq.isEmpty()) {
                    Tuple head = pq.poll();
                    outs.writeObject(head);

                    int streamIndexToRead = tupleMap.get(head);
                    try {
                        Tuple data = readTuple(inputStreams.get(streamIndexToRead));
                        pq.add(data);
                        tupleMap.put(data, streamIndexToRead);
                    } catch (EOFException eof) {
                        inputStreamsEof.set(streamIndexToRead, true);
                    }
                }

                for (ObjectInputStream ins : inputStreams)
                    ins.close();

                outs.close();
                nextRunCount++;
            }

            runCount = nextRunCount;
            nextRunCount = 0;
            runId++;
        }

        return String.format("%s/tmp/%s-%d-0.tbl", currentAbsPath, this.id.toString(), runId - 1);
    }

    /**
     * Wrapper class to read Tuple
     * @param ins ObjectInputStream
     * @return Tuple
     * @throws EOFException
     */
    public Tuple readTuple(ObjectInputStream ins) throws IOException {
        try {
            return (Tuple) ins.readObject();
        } catch (ClassNotFoundException ce) {
            System.out.println("class not found exception --- error in schema object file");
            System.exit(1);

        }
        return null;
    }
}
