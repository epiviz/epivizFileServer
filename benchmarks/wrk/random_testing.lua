-- init random
math.randomseed(os.time())
-- the request function that will run at each request
request = function() 
   
   -- define the path that will search for q=%v 9%v being a random number between 0 and 1000)
   somenumber = math.random(0, 1000)
   somenumber2 = somenumber + 10000
   url_path = "/?requestId=0&version=5&action=getData&seqName=chr2&start=" .. somenumber .. "&end=" .. somenumber2 .. "&measurement=39033&datasource=umd .. math.random(0,1000)"
-- if we want to print the path generated
   print(url_path)
-- Return the request object with the current URL path
   return wrk.format("GET", url_path)
end