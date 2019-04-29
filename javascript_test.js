var msgpack = require("msgpack-lite");
// j
// var xhr = new XMLHttpRequest(); 
// xhr.open("GET", "https://obj.umiacs.umd.edu/tempp/100001.json.testfile"); 
// var blob = null;
// xhr.responseType = "blob";//force the HTTP response, response-type header to be blob
// xhr.onload = function() 
// {
//     blob = xhr.response;//xhr.response is now a blob object
// }
// xhr.send();
// var myReader = new FileReader(); 
// myReader.addEventListener("loadend", function(e){
//     var str = e.srcElement.result;
// });
// myReader.readAsText(blob);
// console.log(myReader)
var request = require("request");
for (var u = 1; u < 6; u++) {
	for (var x = 1; x < 5; x++) { 
		var m = Math.pow(10, u+3) + x;
		var requestSettings = {
			method: 'GET',
			url: "https://obj.umiacs.umd.edu/tempp/" + m.toString() + ".msg.testfile",
			encoding: null
		};

		request(requestSettings, function(error, response, body) {
			var n = (new Date()).getTime();
			var jay = msgpack.decode(body)
			n = (new Date()).getTime() - n;
			console.log(jay);
			console.log("File size");
			console.log(m);
			console.log("Time to load and parse");
			console.log(n);
			console.log("--------------");
		});
	}
}
