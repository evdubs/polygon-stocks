#lang racket/base

(require gregor
         net/http-easy
         racket/cmdline
         threading)

(define api-key (make-parameter ""))

(define extract-date (make-parameter (today)))

(command-line
 #:program "racket ohlc-extract.rkt"
 #:once-each
 [("-d" "--date") date
                  "Date to query. Defaults to today"
                  (extract-date (iso8601->date date))]
 [("-k" "--api-key") key
                     "Polygon API Key"
                     (api-key key)])

(cond [(or (= 0 (->wday (extract-date)))
           (= 6 (->wday (extract-date))))
       (displayln (string-append "Requested date " (date->iso8601 (extract-date)) " falls on a weekend. Terminating."))
       (exit)])

(call-with-output-file* (string-append "/var/local/polygon/ohlc/" (~t (extract-date) "yyyy-MM-dd") ".json")
  (λ (out)
    (with-handlers ([exn:fail?
                     (λ (error)
                       (displayln (string-append "Encountered error for date " (date->iso8601 (extract-date))))
                       (displayln error))])
      (~> (string-append "https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/" (~t (extract-date) "yyyy-MM-dd")
                         "?adjusted=false&include_otc=false"
                         "&apiKey=" (api-key))
          (get _)
          (response-body _)
          (write-bytes _ out))))
  #:exists 'replace)
