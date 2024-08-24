# polygon-stocks
These Racket programs will download data from the [Polygon Stocks API](https://polygon.io/docs/stocks/getting-started) and insert this data into a PostgreSQL database. The intended usage is :

```bash
$ racket ohlc-extract.rkt
$ racket ohlc-transform-load.rkt
```

```bash
$ racket splits-extract.rkt
$ racket splits-transform-load.rkt
```

```bash
$ racket dividends-extract.rkt
$ racket dividends-transform-load.rkt
```

Many of the above programs require a database password. The available parameters are:

```bash
$ racket ohlc-extract.rkt -h
usage: racket ohlc-extract.rkt [ <option> ... ]
<option> is one of
  -d <date>, --date <date>
     Date to query. Defaults to today
  -k <key>, --api-key <key>
     Polygon API Key
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.

$ racket ohlc-transform-load.rkt -h
usage: racket ohlc-transform-load.rkt [ <option> ... ]
<option> is one of
  -b <folder>, --base-folder <folder>
     Polygon OHLC base folder. Defaults to /var/tmp/polygon/ohlc
  -d <date>, --file-date <date>
     Polygon OHLC folder date. Defaults to today
  -n <name>, --db-name <name>
     Database name. Defaults to 'local'
  -p <password>, --db-pass <password>
     Database password
  -u <user>, --db-user <user>
     Database user name. Defaults to 'user'
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.

$ racket dividends-extract.rkt -h
usage: racket dividends-extract.rkt [ <option> ... ]
<option> is one of
  -e <date>, --end-date <date>
     End date of range. Defaults to today
  -f <first>, --first-symbol <first>
     First symbol to query. Defaults to nothing
  -k <key>, --api-key <key>
     Polygon API Key
  -l <last>, --last-symbol <last>
     Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name>
     Database name. Defaults to 'local'
  -p <password>, --db-pass <password>
     Database password
  -s <date>, --start-date <date>
     Start date of range. Defaults to 3 months ago
  -u <user>, --db-user <user>
     Database user name. Defaults to 'user'
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)

 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.

$ racket dividends-transform-load.rkt -h
usage: racket dividends-transform-load.rkt [ <option> ... ]
<option> is one of
  -b <folder>, --base-folder <folder>
     Polygon Stocks dividends base folder. Defaults to /var/tmp/polygon/dividends
  -d <date>, --folder-date <date>
     Polygon Stocks dividends folder date. Defaults to today
  -n <name>, --db-name <name>
     Database name. Defaults to 'local'
  -p <password>, --db-pass <password>
     Database password
  -u <user>, --db-user <user>
     Database user name. Defaults to 'user'
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.

$ racket splits-extract.rkt -h
usage: racket splits-extract.rkt [ <option> ... ]
<option> is one of
  -e <date>, --end-date <date>
     End date of range. Defaults to today
  -f <first>, --first-symbol <first>
     First symbol to query. Defaults to nothing
  -k <key>, --api-key <key>
     Polygon API Key
  -l <last>, --last-symbol <last>
     Last symbol to query. Defaults to nothing
  -n <name>, --db-name <name>
     Database name. Defaults to 'local'
  -p <password>, --db-pass <password>
     Database password
  -s <date>, --start-date <date>
     Start date of range. Defaults to 3 months ago
  -u <user>, --db-user <user>
     Database user name. Defaults to 'user'
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.

$ racket splits-transform-load.rkt -h
usage: racket splits-transform-load.rkt [ <option> ... ]
<option> is one of
  -b <folder>, --base-folder <folder>
     Polygon Stocks splits base folder. Defaults to /var/tmp/polygon/splits
  -d <date>, --folder-date <date>
     Polygon Stocks splits folder date. Defaults to today
  -n <name>, --db-name <name>
     Database name. Defaults to 'local'
  -p <password>, --db-pass <password>
     Database password
  -u <user>, --db-user <user>
     Database user name. Defaults to 'user'
  --help, -h
     Show this help
  --
     Do not treat any remaining argument as a switch (at this level)
 Multiple single-letter switches can be combined after
 one `-`. For example, `-h-` is the same as `-h --`.
```

The provided `schema.sql` file shows the expected schema within the target PostgreSQL instance. This process assumes that you can write to a `/var/tmp/polygon` folder. This process also assumes that you have loaded your database with NASDAQ symbol file information. This data is provided by the [nasdaq-symbols](https://github.com/evdubs/nasdaq-symbols) project.

### Dependencies

It is recommended that you start with the standard Racket distribution. With that, you will need to install the following packages:

```bash
$ raco pkg install --skip-installed gregor http-easy tasks threading
```
