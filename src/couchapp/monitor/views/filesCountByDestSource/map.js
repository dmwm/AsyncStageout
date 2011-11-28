function(doc) {
	if(doc.destination && doc.source){
		emit([doc.destination, doc.source], {"state": doc.state});
	}
}
