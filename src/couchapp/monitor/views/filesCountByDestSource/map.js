function(doc) {
	if(doc.destination && doc.source){
		emit([doc.destination, doc.source, doc.state], 1);
	}
}
