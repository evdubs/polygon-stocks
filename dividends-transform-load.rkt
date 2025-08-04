#lang racket/base

(require db
         gregor
         json
         racket/cmdline
         racket/port
         racket/sequence
         racket/string
         threading)

(define base-folder (make-parameter "/var/local/polygon/dividends"))

(define folder-date (make-parameter (today)))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(command-line
 #:program "racket dividends-transform-load.rkt"
 #:once-each
 [("-b" "--base-folder") folder
                         "Polygon Stocks dividends base folder. Defaults to /var/local/polygon/dividends"
                         (base-folder folder)]
 [("-d" "--folder-date") date
                         "Polygon Stocks dividends folder date. Defaults to today"
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
                (for-each (λ (dividend-hash)
                              (query-exec dbc "
insert into polygon.dividend (
  act_symbol,
  declaration_date,
  ex_date,
  record_date,
  pay_date,
  cash_amount,
  type,
  frequency
) select
  dividend.act_symbol,
  declaration_date,
  ex_date,
  record_date,
  pay_date,
  cash_amount,
  dividend_type,
  frequency
from (
  values (
    $1,
    case when $2 = '' then null else $2::text::date end,
    $3::text::date,
    case when $4 = '' then null else $4::text::date end,
    case when $5 = '' then null else $5::text::date end,
    $6::text::numeric,
    $7,
    $8::text::numeric
  )
) dividend (
    act_symbol,
    declaration_date,
    ex_date,
    record_date,
    pay_date,
    cash_amount,
    dividend_type,
    frequency
  )
join
  nasdaq.symbol
on
  dividend.act_symbol = symbol.act_symbol
on conflict (act_symbol, ex_date) do nothing;
"
                                          (hash-ref dividend-hash 'ticker)
                                          (if (hash-has-key? dividend-hash 'declaration_date)
                                              (hash-ref dividend-hash 'declaration_date)
                                              "")
                                          (hash-ref dividend-hash 'ex_dividend_date)
                                          (if (hash-has-key? dividend-hash 'record_date)
                                              (hash-ref dividend-hash 'record_date)
                                              "")
                                          (if (hash-has-key? dividend-hash 'pay_date)
                                              (hash-ref dividend-hash 'pay_date)
                                              "")
                                          (real->decimal-string (hash-ref dividend-hash 'cash_amount) 6)
                                          (hash-ref dividend-hash 'dividend_type)
                                          (number->string (hash-ref dividend-hash 'frequency))))
                          _))
            (commit-transaction dbc)))))))

(disconnect dbc)
