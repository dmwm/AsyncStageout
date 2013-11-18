function(doc) {
	if(doc.user){
		if (doc.end_time && doc.state=='new') emit([doc.user], {"state": 'resubmitted', "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
		else emit([doc.user], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
	}
}
