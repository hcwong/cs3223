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
        BPlusTree<BPlusTreeKey, Long> index = build(order, tblPath, indexKeys, pageSize, numberOfBuffers);
        String keysString = indexKeys.stream().map((i) -> Integer.toString(i))
            .collect(Collectors.joining("-"));
        try {
            ObjectOutputStream outs = new ObjectOutputStream(
                new FileOutputStream(
                    String.format("%s/indexes/%s-%s", currentAbsPath, tblName, keysString)
                )
            );
            outs.writeObject(index);
        } catch (IOException ioe) {
            System.out.println("Failed to write index to output file");
            System.exit(1);
        }
    }

    public static BPlusTree<BPlusTreeKey, Long> build(
        int order, String tblPath, List<Integer> indexKeys, int pageSize, int numberOfBuffers
    ) {
        int tupleSize = 0;
        String sortedTblPath = "";
        // First sort the tbl so that we can get a clustered index.
        // We assume the .md file and the .tbl file are in the same directory.
        try {
            String mdPath = String.format("%s.md", tblPath.split("[.]", 0)[0]);
            ObjectInputStream schemaIns = new ObjectInputStream(new FileInputStream(mdPath));
            Schema schema = (Schema) schemaIns.readObject();
            tupleSize = schema.getTupleSize();
            ExternalSort sort = new ExternalSort(pageSize, numberOfBuffers);
            sortedTblPath = sort.sort(tblPath, mdPath, indexKeys);
        } catch (IOException ioe) {
            System.out.println("Failed to sort the tbl file in preparation for indexing");
            System.exit(1);
        } catch (ClassNotFoundException ce) {
            System.out.println("Cannot read schema");
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
        long offsetPosition = 0;
        BPlusTree<BPlusTreeKey, Long> index = new BPlusTree<>(order);
        List<BPlusTreeKey> keysSeen = new ArrayList<>();

        while (!eos) {
           try {
               // We read in the file page by page. The value of each leaf node
               // pointer is the offset of the page from the head of the .tbl file
               offsetPosition = fin.getChannel().position();
               for (int i = 0; i < batchSize; i++) {
                   // TODO: Abstract out read tuple to somewhere more appropriate.
                   Tuple tuple = ExternalSort.readTuple(ois);
                   keysSeen.add(buildKey(tuple, indexKeys));
               }

               // Note that the same offset position is used for every tuple in the page
               for (BPlusTreeKey key: keysSeen) {
                   // This check is important. Else if the key already exists,
                   // such as in a case where a key is spread across two blocks
                   // we will overwrite the original offset value
                   if (index.search(key) != null)
                       index.insert(key, offsetPosition);
               }
           } catch (EOFException e) {
               eos = true;
               if (!keysSeen.isEmpty()) {
                   for (BPlusTreeKey key: keysSeen) {
                       index.insert(key, offsetPosition);
                   }
               }
           } catch (IOException ioe) {
               System.out.println("IO Exception when reading tuples");
               System.exit(1);
           }
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
