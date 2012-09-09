function(doc) {
	if(doc.user){
		emit([doc.user, doc.workflow], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
	}
}
