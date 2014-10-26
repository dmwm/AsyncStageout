function(doc) {
	if(doc.workflow){
	   if (doc.end_time && doc.state == 'new') emit([doc.workflow, doc.user], {"state": 'resubmitted'});
           else     emit([doc.workflow, doc.user], {"state": doc.state});

        }
}
