package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import java.util.RandomAccess;
import java.util.WeakHashMap;
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
        int tupleSize = 0;

        String currentAbsPath = Paths.get("").toAbsolutePath().toString();
        String mdPath = String.format("%s.md", tblPath.split("[.]", 0)[0]);
        String keysString = "";
        try {
            ObjectInputStream schemaIns = new ObjectInputStream(new FileInputStream(mdPath));
            Schema schema = (Schema) schemaIns.readObject();
            keysString = indexKeys.stream().map(idx -> schema.getAttribute(idx).getColName())
                .collect(Collectors.joining("-"));
            tupleSize = schema.getTupleSize();
        } catch (IOException ioe) {
            System.exit(1);
        } catch (ClassNotFoundException ce) {
            System.exit(1);
        }

        // Setup the Random Access File that will be written to
        FileChannel fc = null;
        try {
            // A random access table is just the tbl file with an i on the file type.
            fc = new RandomAccessFile(new File(
                String.format("testcases/%s-%s.tbli", tblName, keysString)
            ), "rw")
                .getChannel();
            fc.force(true);
        } catch (FileNotFoundException fofe) {
            System.out.println("Cannot find file");
            System.exit(1);
        } catch (IOException ioe) {
            System.exit(1);
        }


        // Save all files in the a folder called indexes at project root
        try {
            // Turn all the keys into a string
            String indexPath = String.format("%s/indexes/%s-%s", currentAbsPath, tblName, keysString);
            BPlusTree<BPlusTreeKey, Long> index = build(
                order, tblPath, indexKeys, pageSize,
                numberOfBuffers, mdPath, tupleSize, fc);

            // Set the first and last keys here for easy reference later
            index.setFirstKey();
            index.setLastKey();

            ObjectOutputStream outs = new ObjectOutputStream(
                new FileOutputStream(indexPath)
            );
            outs.writeObject(index);
            outs.close();
            fc.close();
        } catch (IOException ioe) {
            System.out.println("Failed to write index to output file");
            System.exit(1);
        }
    }

    public static BPlusTree<BPlusTreeKey, Long> build(
        int order, String tblPath, List<Integer> indexKeys,
        int pageSize, int numberOfBuffers, String mdPath, int tupleSize,
        FileChannel fc
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
        BPlusTree<BPlusTreeKey, Long> index = new BPlusTree<>(order);

        while (!eos) {
            try {
                Tuple tuple = ExternalSort.readTuple(ois);
                byte[] tupleBytes = BuildIndex.serialize(tuple);
                index.serializedValueLength = tupleBytes.length;

                // Now we write to the random access file and store the offset in BPlusTree
                long offset = BuildIndex.addTuple(fc, tupleBytes);
                BPlusTreeKey key = buildKey(tuple, indexKeys);

                if (index.search(key) == null)
                    index.insert(key, offset);

            } catch (EOFException eof) {
                eos = true;
            } catch (IOException ioe) {
                System.out.println("Failed to read from tbl in BuildIndex");
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

    /**
     * Serializes a tuple for addition into the Random Access File
     * @param t
     * @return
     */
    public static byte[] serialize(Tuple t) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(t);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException ioe) {
            System.exit(1);
        }
        return null;
    }

    /**
     * Deserializes a byte array from our random access file to Tuple
     * @param byteArray
     * @return
     */
    public static Tuple deserialize(byte[] byteArray) {
        try {
           ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(byteArray));
           Tuple t = (Tuple) ois.readObject();
           return t;
        } catch (IOException ioe) {
            System.exit(1);
        } catch (ClassNotFoundException coe) {
            System.exit(1);
        }
        return null;
    }

    /**
     * Appends a serialized tuple to the end of our random access file.
     * @param fc
     * @param tupleBytes
     * @return
     */
    public static long addTuple(FileChannel fc, byte[] tupleBytes) {
        try {
            long byteOffset = fc.size();
            fc.position(byteOffset);
            fc.write(ByteBuffer.wrap(tupleBytes));
            return byteOffset;
        } catch (IOException ioe) {
            System.exit(1);
        }

        return -1;
    }

    /**
     * Read a tuple from the Random access file at a given offset
     * @param fc
     * @param offset
     * @param dataSize
     * @return
     */
    public static Tuple readTuple(FileChannel fc, long offset, int dataSize) {
        try {
            // Do not read past the size of the file
            if (offset >= fc.size()) {
                return null;
            }
            byte[] buffer = new byte[dataSize];
            fc.position(offset);
            fc.read(ByteBuffer.wrap(buffer));
            return BuildIndex.deserialize(buffer);
        } catch (IOException ioe) {
            System.exit(1);
        }

        return null;
    }
}
