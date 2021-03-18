# CS3223 Project

## Running a query
All queries (using `QueryMain`) should be run from the `/testcases/` folder, not project root.

## Using the sort file
To sort any file, simply run the ExternalSort class.
The class can be run as such from the project root (it must be run from the project root)
```
java qp.algorithms.ExternalSort <tblpath> <mdpath> <sortindex>
```

`<tblpath>` is the path to the tbl and `<mdpath>` is the path to the corresponding .md file of the table.
The purpose of the mdfile is so that we can get the schema in order to get the tuple size. 
Note that sort defaults to using a pageSize of 100 and 10 for the number of buffers.

Running this sort function will print the absolute path of the sorted tbl to stdout.
By convention, all sorted files will be stored in a `/tmp/` folder at project root, so if it is not present,
do create that folder or there may be unexpected behaviour.

## Building an index

Indexes are not built at runtime. Instead, the user has to build the indexes using the `BuildIndex` class.
This class should also be run from project root.

```
java utils.BuildIndex <tblpath> <tblname> <order> <pageSize> <numberOfBuffers> <pageSize> <numberOfBuffers> 
<keyIndex 1> <keyIndex 2>... 
```

`<tblPath`> is just the path to the table you want to sort and `<tblname>` is just the name of the table.
So for example, if `tblPath` is `testcases/Flights.tbl`, then `tblname` is just `Flights`. 
`<order>` is the order of the B+ Tree. We use a BPlusTree library but have modified it to suit our own needs.
`<numberOfBuffers>` and `<pageSize>` are self-explanatory. `<keyIndex 1>` ... is just the 
indexes of the tuples we want to sort by. So if we have a schema of `Flights.flno, Flights.departureTime`
and `keyIndex` is 0, then we will create an index for `Flights.flno`.

These indexes will be stored in the `/indexes` directory at project root. If you don't have the folder,
please create it or unexpected behavior might occur.

## Changing the join type
The join type used can be changed in the `RandomInitialPlan` file, under the `createJoinOp` function.
There is a line `jn.setJoinType(n)`. Change the value of `n` to the joinType of your choice.

Here are the various JoinType values supported
```
public class JoinType {

    public static final int NESTEDJOIN = 0;
    public static final int BLOCKNESTED = 1;
    public static final int SORTMERGE = 2;
    public static final int INDEXJOIN = 4;
    
    ...
}
```

## Intermediate files
Any query that requires a sort operation will generate many `.tblo` files in testcases.
If it gets a bit too much, you can always just `rm` the files based on the file ending.

## Troubleshooting
If anything fails, just recompile all the files and try again.