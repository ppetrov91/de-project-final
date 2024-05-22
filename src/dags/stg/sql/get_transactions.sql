COPY (SELECT * FROM transactions WHERE transaction_dt BETWEEN %(dt1)s AND %(dt2)s) 
  TO STDOUT (format csv, delimiter ';', ENCODING 'UTF8',header TRUE);

