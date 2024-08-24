CREATE SCHEMA polygon;

CREATE TABLE polygon.ohlc (
  act_symbol text NOT NULL,
  "date" date NOT NULL,
  "open" numeric NULL,
  high numeric NULL,
  low numeric NULL,
  "close" numeric NULL,
  volume int8 NULL,
  CONSTRAINT ohlc_pkey PRIMARY KEY (date, act_symbol),
  CONSTRAINT ohlc_act_symbol_fkey FOREIGN KEY (act_symbol) REFERENCES nasdaq.symbol(act_symbol)
);

CREATE TABLE polygon.split (
  act_symbol text NOT NULL,
  ex_date date NOT NULL,
  to_factor numeric NOT NULL,
  for_factor numeric NOT NULL,
  CONSTRAINT split_pkey PRIMARY KEY (act_symbol, ex_date),
  CONSTRAINT split_act_symbol_fkey FOREIGN KEY (act_symbol) REFERENCES nasdaq.symbol(act_symbol)
);

CREATE TABLE polygon.dividend (
  act_symbol text NOT NULL,
  declaration_date date NULL,
  ex_date date NOT NULL,
  record_date date NULL,
  pay_date date NULL,
  cash_amount numeric NOT NULL,
  "type" text NULL,
  frequency int4 NULL,
  CONSTRAINT dividend_pkey PRIMARY KEY (act_symbol, ex_date),
  CONSTRAINT dividend_act_symbol_fkey FOREIGN KEY (act_symbol) REFERENCES nasdaq.symbol(act_symbol)
);

CREATE OR REPLACE FUNCTION polygon.split_adjusted_ohlc(arg_act_symbol text, arg_start_date date, arg_end_date date, forward boolean)
 RETURNS TABLE(act_symbol text, date date, open numeric, high numeric, low numeric, close numeric, volume numeric)
 LANGUAGE sql
AS $function$
select
  act_symbol,
  date,
  trunc(open * mul(split_ratio), 4) as open,
  trunc(high * mul(split_ratio), 4) as high,
  trunc(low * mul(split_ratio), 4) as low,
  trunc(close * mul(split_ratio), 4) as close,
  trunc(volume / mul(split_ratio), 4) as volume
from
  (select
    o.act_symbol,
    o.date,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    case
      when forward then s.split_ratio
      else 1 / s.split_ratio
    end as split_ratio
  from
    polygon.ohlc o
  left join
    (select
      act_symbol,
      ex_date as date,
      to_factor / for_factor as split_ratio
    from
      polygon.split
    where
      act_symbol = arg_act_symbol and
      ex_date >= arg_start_date) s
  on
    o.act_symbol = s.act_symbol and
    case
      when forward then o.date >= s.date
      else o.date < s.date
    end
  where
    o.act_symbol = arg_act_symbol and
    o.date >= arg_start_date and
    o.date <= arg_end_date) as adjusted_ohlc
group by
  act_symbol, date, open, high, low, close, volume
order by
  act_symbol, date;
$function$
;
