function(head, req) {
	start({
    "headers": {
      "Content-Type": "text/plain"
     }
  });
  //send("<p>");
  //send(toJSON(req));
  //send("</p>");	
  
  var re = new RegExp("^\/store\/temp", "g");
  
  while(row = getRow()) {
  	send(row.key[2] + ":" + row.value + " " + row.key[1] + ":" + row.value.replace(re, "\/store") + "\n");
  }  
}