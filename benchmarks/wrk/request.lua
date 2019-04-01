-- example HTTP POST script which demonstrates setting the
-- HTTP method, body, and adding a header

wrk.method = "GET"
wrk.headers["Content-Type"] = "application/x-www-form-urlencoded"

-- request body in a urlencoded format
-- defines the hierarchy cut and other request params sent to the api.
-- wrk.body = "name=http://obj.umiacs.umd.edu/bigwig-files/39033.bigwig&chr=chr8&start=10550488&end=11554489&points=3"
-- wrk.body = "name=/home/jayaram/.AnnotationHub/39033.bigwig&chr=chr8&start=10550488&end=11554489&points=3"

-- to show the response of the requests, uncomment lines below.
-- function response(status, headers, body)
--   print(body)
-- end
