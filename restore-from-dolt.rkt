#lang racket/base

(require db
         gregor
         racket/cmdline
         racket/string
         racket/system)

(define base-folder (make-parameter "/var/local/dolt/stocks"))

(define start-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define end-date (make-parameter (~t (today) "yyyy-MM-dd")))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket restore-from-dolt.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Base dolt folder. Defaults to /var/local/dolt/stocks"
                         (base-folder folder)]
 [("-e" "--end-date") end
                      "Final date for history retrieval. Defaults to today"
                      (end-date end)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") start
                        "Earliest date for history retrieval. Defaults to today"
                        (start-date start)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])


(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define ohlcv-filename (string-append (base-folder) "/ohlcv-" (start-date) "-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
  act_symbol,
  date,
  open,
  high,
  low,
  close,
  volume
from
  ohlcv
where
  date >= '" (start-date) "' and
  date <= '" (end-date) "';\" > " ohlcv-filename))

(query-exec dbc (string-append "copy polygon.ohlc from '" ohlcv-filename "' (on_error ignore, header match, format csv);"))

(define split-filename (string-append (base-folder) "/split-" (start-date) "-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
  act_symbol,
  ex_date,
  to_factor,
  for_factor
from
  split
where
  ex_date >= '" (start-date) "' and
  ex_date <= '" (end-date) "';\" > " split-filename))

(query-exec dbc (string-append "copy polygon.split from '" split-filename "' (on_error ignore, header match, format csv);"))

(define dividend-filename (string-append (base-folder) "/dividend-" (start-date) "-" (end-date) ".csv"))

(system (string-append "cd " (base-folder) "; "
                       "/usr/local/bin/dolt pull; "
                       "/usr/local/bin/dolt sql -r csv -q \"
select
  act_symbol,
  '' as declaration_date,
  ex_date,
  '' as record_date,
  '' as pay_date,
  amount as cash_amount,
  '' as \"type\",
  '' as frequency
from
  dividend
where
  ex_date >= '" (start-date) "' and
  ex_date <= '" (end-date) "';\" > " dividend-filename))

(query-exec dbc (string-append "copy polygon.dividend from '" dividend-filename "' (on_error ignore, header match, format csv);"))
