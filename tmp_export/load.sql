COPY fct_orders FROM 'tmp_export/fct_orders.csv' (FORMAT 'csv', allow_quoted_nulls false, header 1, delimiter ',', quote '"');
