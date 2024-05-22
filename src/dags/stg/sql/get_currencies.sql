COPY (SELECT * FROM currencies WHERE date_update BETWEEN %(dt1)s AND %(dt2)s) 
  TO STDOUT (format csv, delimiter ';', ENCODING 'UTF8',header TRUE);
