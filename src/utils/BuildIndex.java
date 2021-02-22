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
import qp.utils.Tuple;

/**
 * BuildIndex allows us to build an index from a .tbl file
 * The index generated will be a B+ Tree index
 * In our DB we assume that the indexes can all be loaded into memory (in-memory)
 */
public class BuildIndex {
    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: java BuildIndex <tblpath> " +
                "<tblname> <order> <key index 1> <key index 2> ...");
            return;
        }

        String tblPath = args[0];
        String tblName = args[1]; // In case the tblpath is not in the same directory as cwd.
        int order = Integer.parseInt(args[2]);
        List<Integer> indexKeys = new ArrayList<>();
        for (int i = 3; i < args.length; i++)
            indexKeys.add(Integer.parseInt(args[i]));

        // Save all files in the a folder called indexes at project root
        String currentAbsPath = Paths.get("").toAbsolutePath().toString();
        BPlusTree<BPlusTreeKey, Long> index = build(order, tblPath, indexKeys);
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
        int order, String tblPath, List<Integer> indexKeys
    ) {
        FileInputStream fin = null;
        ObjectInputStream ois = null;
        try {
            fin = new FileInputStream(tblPath);
            ois = new ObjectInputStream(fin);
        } catch (IOException ioe) {
            System.out.println("IO Exception reading file");
            System.exit(1);
        }

        boolean eos = false;
        // TODO: Calculate the key size based on the page size and key size and pointer size
        BPlusTree<BPlusTreeKey, Long> index = new BPlusTree<>(order);

        while (!eos) {
           try {
               // TODO: Abstract out read tuple to somewhere more appropriate.
               Tuple tuple = ExternalSort.readTuple(ois);
               BPlusTreeKey bTreeKey = buildKey(tuple, indexKeys);
               index.insert(bTreeKey, fin.getChannel().position());
           } catch (EOFException e) {
               eos = true;
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
