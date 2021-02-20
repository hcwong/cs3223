package utils;

import java.io.*;
import qp.utils.*;

/**
 * This is a util file for developers to print out the tuples in a .tbl file for inspection.
 */
public class PrintTupleFile {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
           System.out.println("Usage: PrintTupleFile <tbl file here>");
           return;
        }

        try {
            ObjectInputStream ins = new ObjectInputStream(new FileInputStream(args[0]));

            while (true) {
                Tuple tuple = (Tuple) ins.readObject();
                System.out.println(tuple);
            }
        } catch (EOFException eof) {
            System.out.println("Done printing file");
            return;
        } catch (ClassNotFoundException ce) {
            System.out.println("Class not found exception");
            System.exit(1);
        }
    }
}
