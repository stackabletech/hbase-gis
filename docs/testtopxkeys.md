Assume this is a table with 4-byte rowkeys

    a1bf
    a2be
    a3bd
    a4bc
    a5bb
    a6ca
    a7c9
    a8c8
    b1a7
    ...

Now, I make a Scan with startRow="a" and endRow="b" and limit 5.
An unconditional scan with these params wound return these rows:

    a1bf
    a2be
    a3bd
    a4bc
    a5bb

Ok ...
So now I want to provide an offset and a length into the rowkey which determines some bytes that become my "bin-id" (for
lack of a better word.)
So if I provide offset=2 and length=1, the third byte of the rowkey will be my "bin-id".
Now I want the scanner to return only the first N results for every distinct bin-id it encounters.

Given:
offset=2
length=1
With N = 1, I would expect these results:

    a1bf
    a6ca

With N = 2, I would expect these results:

    a1bf
    a2be
    a6ca
    a7c9

With N = 3, I would expect these results:

    a1bf
    a2be
    a3bd
    a6ca
    a7c9

(remember the limit=5 of the scan should still be respected)