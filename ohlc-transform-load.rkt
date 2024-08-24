#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/list
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/tmp/polygon/ohlc"))

(define file-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket ohlc-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Polygon OHLC base folder. Defaults to /var/tmp/polygon/ohlc"
                         (base-folder folder)]
 [("-d" "--file-date") date
                         "Polygon OHLC folder date. Defaults to today"
                         (file-date (iso8601->date date))]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(call-with-input-file (string-append (base-folder) "/" (~t (file-date) "yyyy-MM-dd") ".json")
  (λ (in)
    (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to load file for date " (~t (file-date) "yyyy-MM-dd")))
                                 (displayln e))])
      (~> (port->string in)
          (string->jsexpr _)
          (hash-ref _ 'results)
          (for-each (λ (el)
                      (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to insert " (hash-ref el 'T)
                                                                                  " into database for date " (~t (file-date) "yyyy-MM-dd")))
                                                 (displayln e))])
                        (query-exec dbc "
with symbols as (
  select
    act_symbol
  from
    nasdaq.symbol
  where
    cqs_symbol = $1 or
    nasdaq_symbol = $1 and
    (last_seen >= $2::text::date or
    last_seen = (select max(last_seen) from nasdaq.symbol))
)
insert into polygon.ohlc (
  act_symbol,
  date,
  open,
  high,
  low,
  close,
  volume
) values (
  (select act_symbol from symbols),
  $2::text::date,
  $3::text::numeric,
  $4::text::numeric,
  $5::text::numeric,
  $6::text::numeric,
  $7::text::numeric
) on conflict (act_symbol, date) do update set
  open = $3::text::numeric,
  high = $4::text::numeric,
  low = $5::text::numeric,
  close = $6::text::numeric,
  volume = $7::text::numeric;
"
                                  (hash-ref el 'T)
                                  (moment->iso8601 (posix->moment (/ (hash-ref el 't) 1000) "America/New_York"))
                                  (real->decimal-string (hash-ref el 'o) 4)
                                  (real->decimal-string (hash-ref el 'h) 4)
                                  (real->decimal-string (hash-ref el 'l) 4)
                                  (real->decimal-string (hash-ref el 'c) 4)
                                  (number->string (hash-ref el 'v))))) _)))))
