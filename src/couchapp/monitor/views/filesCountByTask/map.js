function(doc) {
	if(doc.workflow){
	   if (doc.end_time && doc.state == 'new') emit([doc.workflow], {"state": 'resubmitted', "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});	
           else     emit([doc.workflow], {"state": doc.state, "user": doc.user, "destination": doc.destination, "source": doc.source, "id": doc.lfn});

        }
}
