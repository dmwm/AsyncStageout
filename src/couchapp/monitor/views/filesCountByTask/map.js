function(doc) {
	if(doc.task){
		emit([doc.task], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc._id});
	}
}
