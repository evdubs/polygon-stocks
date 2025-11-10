#lang racket/base

(require db
         gregor
         json
         net/http-easy
         racket/cmdline
         racket/file
         racket/list
         racket/port
         racket/string
         tasks
         threading
         "list-partition.rkt")

(define (download-dividends symbols)
  (make-directory* (string-append "/var/local/polygon/dividends/" (end-date)))
  (call-with-output-file* (string-append "/var/local/polygon/dividends/" (end-date) "/"
                                         (first symbols) "-" (last symbols) ".json")
    (λ (out)
      (with-handlers ([exn:fail?
                       (λ (error)
                         (displayln (string-append "Encountered error for " (first symbols) "-" (last symbols)))
                         (displayln error))])
        (define body-js (~> (string-append "https://api.massive.com/v3/reference/dividends?limit=1000&sort=ex_dividend_date"
                                        "&apiKey=" (api-key)
                                        "&ex_dividend_date.gte=" (start-date)
                                        "&ex_dividend_date.lte=" (end-date)
                                        "&ticker.gte=" (first symbols)
                                        "&ticker.lte=" (last symbols))
                         (get _)
                         (response-json _)))
        (cond [(hash-has-key? body-js 'next_url)
               (define second-body-js (~> (string-append (hash-ref body-js 'next_url)
                                                         "&apiKey=" (api-key))
                                          (get _)
                                          (response-json _)))
               (cond [(hash-has-key? second-body-js 'next_url) (displayln "Found another Next URL. Results will be missing.")])
               (write-bytes (jsexpr->bytes (hash 'results (append (hash-ref body-js 'results)
                                                                  (hash-ref second-body-js 'results))
                                                 'request_id (hash-ref body-js 'request_id)
                                                 'status (hash-ref body-js 'status)))
                            out)]
              [else (write-bytes (jsexpr->bytes body-js) out)])))
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
 #:program "racket dividends-extract.rkt"
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

(define grouped-symbols (list-partition symbols 50 50))

(define delay-interval 20)

(define delays (map (λ (x) (* delay-interval x)) (range 0 (length grouped-symbols))))

(with-task-server (for-each (λ (l) (schedule-delayed-task (λ () (thread (λ () (download-dividends (first l)))))
                                                          (second l)))
                            (map list grouped-symbols delays))
  ; add a final task that will halt the task server
  (schedule-delayed-task (λ () (schedule-stop-task)) (* delay-interval (length delays)))
  (run-tasks))
