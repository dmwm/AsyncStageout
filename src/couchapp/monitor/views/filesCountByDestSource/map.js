function(doc) {
	if(doc.state){
		emit([doc.destination, doc.source], {"state": doc.state});
	}
}
