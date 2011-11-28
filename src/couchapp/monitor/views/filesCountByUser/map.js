function(doc) {
	if(doc.user){
		emit([doc.user, doc.task], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
	}
}
