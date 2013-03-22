function(doc) {
	if(doc.workflow){
                emit([doc.workflow], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});
        }
}
