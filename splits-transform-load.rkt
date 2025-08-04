#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/local/polygon/splits"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket splits-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Polygon Stocks splits base folder. Defaults to /var/local/polygon/splits"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Polygon Stocks splits folder date. Defaults to today"
                         (folder-date (iso8601->date date))]
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

(parameterize ([current-directory (string-append (base-folder) "/" (~t (folder-date) "yyyy-MM-dd") "/")])
  (for ([p (sequence-filter (λ (p) (string-contains? (path->string p) ".json")) (in-directory (current-directory)))])
    (let* ([file-name (path->string p)]
           [ticker-range (string-replace (string-replace file-name (path->string (current-directory)) "") ".json" "")])
      (call-with-input-file file-name
        (λ (in)
          (with-handlers ([exn:fail? (λ (e) (displayln (string-append "Failed to process "
                                                                      ticker-range
                                                                      " for date "
                                                                      (~t (folder-date) "yyyy-MM-dd")))
                                       (displayln e)
                                       (rollback-transaction dbc))])
            (start-transaction dbc)
            (~> (port->string in)
                (string->jsexpr _)
                (hash-ref _ 'results)
                (for-each (λ (split-hash)
                              (query-exec dbc "
insert into polygon.split (
  act_symbol,
  ex_date,
  to_factor,
  for_factor
) select
  split.act_symbol,
  split.ex_date,
  split.to_factor,
  split.for_factor
from (
  values (
    $1,
    $2::text::date,
    $3::text::numeric,
    $4::text::numeric
  )
) split (
    act_symbol,
    ex_date,
    to_factor,
    for_factor
  )
join
  nasdaq.symbol
on
  split.act_symbol = symbol.act_symbol
on conflict (act_symbol, ex_date) do nothing;
"
                                          (hash-ref split-hash 'ticker)
                                          (hash-ref split-hash 'execution_date)
                                          (real->decimal-string (hash-ref split-hash 'split_to) 6)
                                          (real->decimal-string (hash-ref split-hash 'split_from) 6)))
                          _))
            (commit-transaction dbc)))))))

(disconnect dbc)
