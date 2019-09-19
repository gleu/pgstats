README
======

This repository contains the source of three executables.

pgstat is a vmstat-like tool for PostgreSQL.

pgcsvstat outputs PostgreSQL statistics views into CSV files. The idea is that
you can load them on any spreadsheet to get the graphs you want.

pgdisplay is 

Requirements
------------

You only need the libpq library, PostgreSQL header files, and the pg_config
tool. The header files and the tool are usually available in a -dev package.

Compilation
-----------

You only have to do:

```
make
make install
```

Usage
-----

Use --help to get informations on all command line options for these three
tools.

More information on pgstat
--------------------------

pgstat is an online command tool that connects to a database and grabs its
activity statistics. As PostgreSQL has many statistics, you have a command
switch to choose the one you want (-s):

* archiver for pg_stat_archiver
* bgwriter for pg_stat_bgwriter
* connection for connections by type (9.1+)
* database for pg_stat_database
* table for pg_stat_all_tables
* tableio for pg_statio_all_tables
* index for pg_stat_all_indexes
* function for pg_stat_user_function
* statement for pg_stat_statements
* xlog for xlog writes (9.2+)
* tempfile for temporary file usage
* pbpools for pgBouncer pools statistics
* pbstats for pgBouncer general statistics

It looks a lot like vmstat. You ask it the statistics you want, and the
frequency to gather these statistics. Just like this:

```
$ pgstat -s connection
 - total - active - lockwaiting - idle in transaction - idle -
    1546       15             0                     0   1531
    1544       17             0                     0   1527
    1544       14             0                     0   1530
    1546       26             0                     0   1520
    1543       21             0                     0   1522
```

Yeah, way too many idle connections. Actually, way too many connections.
Definitely needs a pooler there.

This is what happens on a 10-secondes 10-clients pgbench test:

```
$ pgstat -s database 1
- backends - ------ xacts ------ -------------- blocks -------------- -------------- tuples -------------- ------ temp ------ ------- misc --------
                commit rollback     read    hit read_time write_time      ret    fet    ins    upd    del    files     bytes   conflicts deadlocks
         1      224041       17    24768 2803774         0          0   4684398 234716 2105701  16615    113        1  14016512           0         0
         1           0        0        0      0         0          0        0      0      0      0      0        0         0           0         0
         1           3        0        0    205         0          0       92     92      0      0      0        0         0           0         0
        11          20        0        0    500         0          0     1420    184      0      1      0        0         0           0         0
        11          69        0        1   4438         0          0     1736    986     68    204      0        0         0           0         0
        11         136        0       12   4406         0          0     1767    270    135    405      0        0         0           0         0
        11         108        0        0   3434         0          0     1394    214    107    321      0        0         0           0         0
        11          96        0        0   3290         0          0     1240    190     95    285      0        0         0           0         0
        11         125        0        0   4045         0          0     1620    248    124    372      0        0         0           0         0
        11         126        0        0   4222         0          0     1628    250    125    375      0        0         0           0         0
        11         111        0        0   3644         0          0     1436    220    110    330      0        0         0           0         0
        11          78        0        0   2549         0          0     1918    161     75    225      0        0         0           0         0
        11         118        0        0   3933         0          0     1524    234    117    351      0        0         0           0         0
         1         130        0        0   4276         0          0     1685    258    129    387      0        0         0           0         0
         1           1        0        0      0         0          0        0      0      0      0      0        0         0           0         0
         1           1        0        0      0         0          0        0      0      0      0      0        0         0           0         0
         1           1        0        0      0         0          0        0      0      0      0      0        0         0           0         0
```

You clearly see when it starts, when it stops, and what it did during the 10
seconds. You can filter on a specific database with the -f command line
option. Here is what happens at the tables level:

```
$ pgstat -s table -d b1 1
-- sequential -- ------ index ------ ----------------- tuples -------------------------- -------------- maintenance --------------
   scan  tuples     scan  tuples         ins    upd    del hotupd   live   dead analyze   vacuum autovacuum analyze autoanalyze
  68553  1467082   264957  266656      7919869  59312    113  57262 4611779   3782   5401      22         10       4          22
      3     430        0       0           0      0      0      0      0      0      0       0          0       0           0
      3     430        0       0           0      0      0      0      0      0      0       0          0       0           0
    231    2351     1116    1222          61    184      0    180     61    124    245       2          0       0           0
    431    1750      240     240         120    360      0    358    120    242    480       0          0       0           0
    385    1640      220     220         110    330      0    327    110     11    440       0          0       0           0
    340    1475      190     190          95    285      0    285     95    189    380       0          0       0           0
    398    1651      222     222         111    333      0    331    111     -2    444       0          0       0           0
    353    1519      198     198          99    297      0    293     99    200    396       0          0       0           0
    335    1453      186     186          93    279      0    274     93   -210    372       0          0       0           0
    446    1838      256     256         128    384      0    381    128    104    512       0          0       0           0
    425    1739      238     238         119    357      0    354    119    241    476       0          0       0           0
    360    1552      204     204         102    306      0    305    102    -10    408       0          0       0           0
    386    1629      218     218         109    327      0    325    109     57    436       0          0       0           0
    437    1761      242     242         121    363      0    363    121   -292    484       0          0       0           0
    373    1563      206     206         103    309      0    305    103     -1    412       0          0       0           0
    323    1442      184     184          92    276      0    273     92    188    368       0          0       0           0
    412    1706      232     232         116    348      0    346    116     76    464       0          0       0           0
    291    1332      164     164          82    246      0    245     82   -216    328       0          0       0           0
    189    1013      106     106          53    159      0    158     53    106    212       0          0       0           0
    346    1508      196     196          98    294      0    290     98    -18    392       0          0       0           0
    304    1376      172     172          86    258      0    258     86   -156    344       0          0       0           0
    442    1794      248     248         124    372      0    368    124   -260    496       0          0       0           0
      9    1371      157     260           0     13      0     13 -11602   -329  -6053       0          2       0           3
      3     430        0       0           0      0      0      0      0      0      0       0          0       0           0
      3     430        0       0           0      0      0      0      0      0      0       0          0       0           0
```

You can also filter by table name with the -f command line switch:

```
$ pgstat -s table -d b1 -f pgbench_history 1
-- sequential -- ------ index ------ ----------------- tuples -------------------------- -------------- maintenance --------------
   scan  tuples     scan  tuples         ins    upd    del hotupd   live   dead analyze   vacuum autovacuum analyze autoanalyze
      0       0        0       0       21750      0      0      0   2022      0      0       1          0       1           7
      0       0        0       0           0      0      0      0      0      0      0       0          0       0           0
      0       0        0       0          64      0      0      0     64      0     64       0          0       0           0
      0       0        0       0         122      0      0      0    122      0    122       0          0       0           0
      0       0        0       0         106      0      0      0    106      0    106       0          0       0           0
      0       0        0       0          99      0      0      0     99      0     99       0          0       0           0
      0       0        0       0          88      0      0      0     88      0     88       0          0       0           0
      0       0        0       0         116      0      0      0    116      0    116       0          0       0           0
      0       0        0       0          99      0      0      0     99      0     99       0          0       0           0
      0       0        0       0          61      0      0      0     61      0     61       0          0       0           0
      0       0        0       0          42      0      0      0     42      0     42       0          0       0           0
      0       0        0       0         106      0      0      0    106      0    106       0          0       0           0
      0       0        0       0          55      0      0      0     55      0     55       0          0       0           0
      0       0        0       0         121      0      0      0    121      0    121       0          0       0           0
      0       0        0       0          68      0      0      0  -1942      0  -1011       0          0       0           1
      0       0        0       0          99      0      0      0     99      0     99       0          0       0           0
      0       0        0       0         109      0      0      0    109      0    109       0          0       0           0
      0       0        0       0          94      0      0      0     94      0     94       0          0       0           0
      0       0        0       0         120      0      0      0    120      0    120       0          0       0           0
      0       0        0       0         110      0      0      0    110      0    110       0          0       0           0
      0       0        0       0         100      0      0      0    100      0    100       0          0       0           0
      0       0        0       0         115      0      0      0    115      0    115       0          0       0           0
      0       0        0       0           0      0      0      0      0      0      0       0          0       0           0
      0       0        0       0           0      0      0      0      0      0      0       0          0       0           0
```

We see that the activity on this table is quite different from what happens to
the other tables.

There's also a report from the pg_stat_statements extension. It works pretty well:

```
$ pgstat -s statement -d b1
--------- misc ---------- ----------- shared ----------- ----------- local ----------- ----- temp ----- -------- time --------
  calls      time   rows      hit   read  dirty written      hit   read  dirty written    read written        read   written
 383843   1756456.50 13236523   9277049  38794  50915    1640   1008844  17703   8850    8850    1711    1711        0.00      0.00
      1     0.75      1        0      0      0       0        0      0      0       0       0       0        0.00      0.00
      1     0.50      1        0      0      0       0        0      0      0       0       0       0        0.00      0.00
      1     0.75      1        0      0      0       0        0      0      0       0       0       0        0.00      0.00
    310   2709.88    220     1527     10     63       0        0      0      0       0       0       0        0.00      0.00
    797   8555.00    569     3736     10    109       0        0      0      0       0       0       0        0.00      0.00
    725   9215.25    519     3610     23    115       0        0      0      0       0       0       0        0.00      0.00
    266   7729.38    190     1257      2     43       0        0      0      0       0       0       0        0.00      0.00
    831   10196.12    594     3988     11    112       0        0      0      0       0       0       0        0.00      0.00
    788   8678.38    563     3803      8     92       0        0      0      0       0       0       0        0.00      0.00
    736   9080.62    526     3616      7     89       0        0      0      0       0       0       0        0.00      0.00
    792   8395.50    566     3742     11     96       0        0      0      0       0       0       0        0.00      0.00
    814   9346.75    582     3985      9     84       0        0      0      0       0       0       0        0.00      0.00
    763   8941.12    545     3799      9     84       0        0      0      0       0       0       0        0.00      0.00
    728   8543.25    520     3549      8     62       0        0      0      0       0       0       0        0.00      0.00
    589   9143.62    421     2812      7     45       0        0      0      0       0       0       0        0.00      0.00
    785   8710.00    561     3788      4     60       0        0      0      0       0       0       0        0.00      0.00
    785   9117.25    561     3885      4     60       0        0      0      0       0       0       0        0.00      0.00
    785   8397.12    561     3788      1     52       0        0      0      0       0       0       0        0.00      0.00
    799   9398.12    571     3925      7     60       0        0      0      0       0       0       0        0.00      0.00
    765   9033.88    547     3757      3     43       0        0      0      0       0       0       0        0.00      0.00
    805   8663.25    575     3886      6     57       0        0      0      0       0       0       0        0.00      0.00
    765   8490.50    547     3661      7     39       0        0      0      0       0       0       0        0.00      0.00
    764   8850.00    546     3698      4     41       0        0      0      0       0       0       0        0.00      0.00
    396   6706.50    283     1992      1     14       0        0      0      0       0       0       0        0.00      0.00
      1     0.38      1        0      0      0       0        0      0      0       0       0       0        0.00      0.00
      1     0.62      1        0      0      0       0        0      0      0       0       0       0        0.00      0.00
```

You can filter a specific statement by its query id.

Of course, it first searches for the extension, and complains if it isn't there:

```
$ pgstat -s statement -d b2
pgstat: Cannot find the pg_stat_statements extension.
```

One of my customers had a lot of writes on their databases, and I wanted to
know how much writes occured in the WAL files. vmstat would only tell me how
much writes on all files, but I was only interested in WAL writes. So I added
a new report that grabs the current XLOG position, and diff it with the
previous XLOG position. It gives something like this with a pgbench test:

```
$ ./pgstat -s xlog
-------- filename -------- -- location -- ---- bytes ----
 00000001000000000000003E   0/3EC49940        1053071680
 00000001000000000000003E   0/3EC49940                 0
 00000001000000000000003E   0/3EC49940                 0
 00000001000000000000003E   0/3EC875F8            253112
 00000001000000000000003E   0/3ED585C8            856016
 00000001000000000000003E   0/3EE36C40            910968
 00000001000000000000003E   0/3EEFCC58            811032
 00000001000000000000003E   0/3EFAB9D0            716152
 00000001000000000000003F   0/3F06A3C0            780784
 00000001000000000000003F   0/3F0E79E0            513568
 00000001000000000000003F   0/3F1354E0            318208
 00000001000000000000003F   0/3F1F6218            789816
 00000001000000000000003F   0/3F2BCE00            814056
 00000001000000000000003F   0/3F323240            418880
 00000001000000000000003F   0/3F323240                 0
 00000001000000000000003F   0/3F323240                 0
```

That's not big numbers, so it's easy to find it writes at 253K/s, but if the
number were bigger, it might get hard to read. One of my co-worker, Julien
Rouhaud, added a human readable option:

```
$ ./pgstat -s xlog -H
-------- filename -------- -- location -- ---- bytes ----
 00000001000000000000003F   0/3F32EDC0      1011 MB
 00000001000000000000003F   0/3F32EDC0      0 bytes
 00000001000000000000003F   0/3F32EDC0      0 bytes
 00000001000000000000003F   0/3F3ABC78      500 kB
 00000001000000000000003F   0/3F491C10      920 kB
 00000001000000000000003F   0/3F568548      858 kB
 00000001000000000000003F   0/3F634748      817 kB
 00000001000000000000003F   0/3F6F4378      767 kB
 00000001000000000000003F   0/3F7A56D8      709 kB
 00000001000000000000003F   0/3F8413D0      623 kB
 00000001000000000000003F   0/3F8D7590      600 kB
 00000001000000000000003F   0/3F970160      611 kB
 00000001000000000000003F   0/3F9F2840      522 kB
 00000001000000000000003F   0/3FA1FD88      181 kB
 00000001000000000000003F   0/3FA1FD88      0 bytes
 00000001000000000000003F   0/3FA1FD88      0 bytes
 00000001000000000000003F   0/3FA1FD88      0 bytes
```

That's indeed much more readable if you ask me.

Another customer wanted to know how many temporary files were written, and
their sizes. Of course, you can get that with the pg_stat_database view, but
it only gets added when the query is done. We wanted to know when the query is
executed. So I added another report:

```
$ ./pgstat -s tempfile
--- size --- --- count ---
         0             0
         0             0
  13082624             1
  34979840             1
  56016896             1
  56016896             1
  56016896             1
         0             0
         0             0
```

You see the file being stored.

Ideas
-----

pg_stat_archiver: display the current wal and the last archived
pg_stat_archiver: display the duration since the last archived wal
pg_stat_*_tables: display the duration since the last *vacuum and *analyze
