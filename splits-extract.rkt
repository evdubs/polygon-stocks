#lang racket/base

(require db
         gregor
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         tasks
         threading
         "list-partition.rkt")

(define (download-splits symbols)
  (make-directory* (string-append "/var/tmp/polygon/splits/" (end-date)))
  (call-with-output-file* (string-append "/var/tmp/polygon/splits/" (end-date) "/"
                                         (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols)))
                         (displayln error))])
        (~> (string-append "https://api.polygon.io/v3/reference/splits?limit=1000&sort=execution_date"
                           "&apiKey=" (api-key)
                           "&execution_date.gte=" (start-date)
                           "&execution_date.lte=" (end-date)
                           "&ticker.gte=" (first symbols)
                           "&ticker.lte=" (last symbols))
            (get _)
            (response-body _)
            (write-bytes _ out))))
    #:exists 'replace))

(define api-key (make-parameter ""))

(define db-user (make-parameter "user"))

(define db-name (make-parameter "local"))

(define db-pass (make-parameter ""))

(define end-date (make-parameter (date->iso8601 (today))))

(define first-symbol (make-parameter ""))

(define last-symbol (make-parameter ""))

(define start-date (make-parameter (date->iso8601 (-months (today) 1))))

(command-line
 #:program "racket splits-extract.rkt"
 #:once-each
 [("-e" "--end-date") date
                      "End date of range. Defaults to today"
                      (end-date date)]
 [("-f" "--first-symbol") first
                          "First symbol to query. Defaults to nothing"
                          (first-symbol first)]
 [("-k" "--api-key") key
                     "Polygon API Key"
                     (api-key key)]
 [("-l" "--last-symbol") last
                         "Last symbol to query. Defaults to nothing"
                         (last-symbol last)]
 [("-n" "--db-name") name
                     "Database name. Defaults to 'local'"
                     (db-name name)]
 [("-p" "--db-pass") password
                     "Database password"
                     (db-pass password)]
 [("-s" "--start-date") date
                        "Start date of range. Defaults to 3 months ago"
                        (start-date date)]
 [("-u" "--db-user") user
                     "Database user name. Defaults to 'user'"
                     (db-user user)])

(define dbc (postgresql-connect #:user (db-user) #:database (db-name) #:password (db-pass)))

(define symbols (query-list dbc "
select
  act_symbol
from
  nasdaq.symbol
where
  is_test_issue = false and
  is_next_shares = false and
  nasdaq_symbol !~ '[-\\$\\+\\*#!@%\\^=~]' and
  case when nasdaq_symbol ~ '[A-Z]{4}[L-Z]'
    then security_name !~ '(Note|Preferred|Right|Unit|Warrant)'
    else true
  end and
  last_seen >= (select max(last_seen) from nasdaq.symbol where last_seen <= $3::text::date) and
  case when $1 != ''
    then act_symbol >= $1
    else true
  end and
  case when $2 != ''
    then act_symbol <= $2
    else true
  end
order by
  act_symbol;
"
                            (first-symbol)
                            (last-symbol)
                            (end-date)))

(disconnect dbc)

(define grouped-symbols (list-partition symbols 100 100))

(define delay-interval 15)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-splits (first l)))))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
