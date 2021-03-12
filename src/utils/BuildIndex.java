package utils;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import java.util.stream.Collectors;
import qp.algorithms.ExternalSort;
import qp.utils.BPlusTree;
import qp.utils.BPlusTreeKey;
import qp.utils.Schema;
import qp.utils.Tuple;

/**
 * BuildIndex allows us to build an index from a .tbl file
 * The index generated will be a B+ Tree index
 * In our DB we assume that the indexes can all be loaded into memory (in-memory)
 * Our leaf nodes store the keys and the values are the offset of the page
 * the tuple is in from the file head.
 * This is an unclustered index
 */
public class BuildIndex {
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: java BuildIndex <tblpath> " +
                "<tblname> <order> <pageSize> <numberOfBuffers> <key index 1> <key index 2> ...");
            return;
        }

        String tblPath = args[0];
        String tblName = args[1]; // In case the tblpath is not in the same directory as cwd.
        int order = Integer.parseInt(args[2]);
        int pageSize = Integer.parseInt(args[3]);
        int numberOfBuffers = Integer.parseInt(args[4]);
        List<Integer> indexKeys = new ArrayList<>();
        for (int i = 5; i < args.length; i++)
            indexKeys.add(Integer.parseInt(args[i]));

        // Save all files in the a folder called indexes at project root
        String currentAbsPath = Paths.get("").toAbsolutePath().toString();
        try {
            String mdPath = String.format("%s.md", tblPath.split("[.]", 0)[0]);
            ObjectInputStream schemaIns = new ObjectInputStream(new FileInputStream(mdPath));
            Schema schema = (Schema) schemaIns.readObject();
            int tupleSize = schema.getTupleSize();
            String keysString = indexKeys.stream().map(idx -> schema.getAttribute(idx).getColName())
                .collect(Collectors.joining("-"));
            String indexPath = String.format("%s/indexes/%s-%s", currentAbsPath, tblName, keysString);
            BPlusTree<BPlusTreeKey, String> index = build(
                order, tblPath, indexKeys, pageSize,
                numberOfBuffers, mdPath, tupleSize, indexPath);

            // Set the first and last keys here for easy reference later
            index.setFirstKey();
            index.setLastKey();

            ObjectOutputStream outs = new ObjectOutputStream(
                new FileOutputStream(indexPath)
            );
            outs.writeObject(index);
            outs.close();
        } catch (IOException ioe) {
            System.out.println("Failed to write index to output file");
            System.exit(1);
        } catch (ClassNotFoundException ce) {
            System.out.println("Cannot read schema");
            System.exit(1);
        }
    }

    public static BPlusTree<BPlusTreeKey, String> build(
        int order, String tblPath, List<Integer> indexKeys,
        int pageSize, int numberOfBuffers, String mdPath, int tupleSize,
        String indexPath
    ) {
        String sortedTblPath = "";
        // We assume the .md file and the .tbl file are in the same directory.
        try {
            ExternalSort sort = new ExternalSort(pageSize, numberOfBuffers);
            // Generates the sorted table
            sortedTblPath = sort.sort(tblPath, mdPath, indexKeys);
        } catch (IOException ioe) {
            System.out.println("Failed to sort the tbl file in preparation for indexing");
            System.exit(1);
        }

        FileInputStream fin = null;
        ObjectInputStream ois = null;
        try {
            fin = new FileInputStream(sortedTblPath);
            ois = new ObjectInputStream(fin);
        } catch (IOException ioe) {
            System.out.println("IO Exception reading file");
            System.exit(1);
        }

        int batchSize = (int) Math.floor(pageSize / tupleSize);
        assert(batchSize > 2);

        boolean eos = false;
        BPlusTree<BPlusTreeKey, String> index = new BPlusTree<>(order);
        List<Tuple> batch = new ArrayList<>();
        int batchCount = 0;

        while (!eos) {
           try {
               for (int i = 0; i < batchSize; i++) {
                   // TODO: Abstract out read tuple to somewhere more appropriate.
                   Tuple tuple = ExternalSort.readTuple(ois);
                   batch.add(tuple);
               }

               String batchFile = String.format("%s-%d", indexPath, batchCount);
               ObjectOutputStream batchOos = new ObjectOutputStream(new FileOutputStream(batchFile));
               for (Tuple tuple: batch) {
                   BPlusTreeKey key = buildKey(tuple, indexKeys);
                   // Do not insert into the index if it already exists
                   if (index.search(key) == null)
                       index.insert(key, batchFile);

                   batchOos.writeObject(tuple);
               }
               batch.clear();
               batchCount++;
           } catch (EOFException e) {
               eos = true;
           } catch (IOException ioe) {
               System.out.println("IO Exception when reading tuples");
               System.exit(1);
           }
        }

        // Insert the last batch
        String batchFile = String.format("%s-%d", indexPath, batchCount);
        try {
            ObjectOutputStream batchOos = new ObjectOutputStream(new FileOutputStream(batchFile));
            for (Tuple tuple: batch) {
                BPlusTreeKey key = buildKey(tuple, indexKeys);
                // Do not insert into the index if it already exists
                if (index.search(key) == null)
                    index.insert(key, batchFile);

                batchOos.writeObject(tuple);
            }
        } catch (IOException ioe) {
            System.out.println("Failed to write to batch");
            System.exit(1);
        }

        try {
            ois.close();
            fin.close();
        } catch (IOException ioe) {
            System.exit(1);
        }

        return index;
    }

    public static BPlusTreeKey buildKey(Tuple tuple, List<Integer> indexKeys) {
        if (indexKeys.size() > tuple.data().size()) {
            System.out.println("Error ");
            System.exit(1);
        }

        List<Object> keys = new ArrayList<>();
        for (Integer i: indexKeys)
            keys.add(tuple.data().get(i));

        return new BPlusTreeKey(keys);
    }
}
