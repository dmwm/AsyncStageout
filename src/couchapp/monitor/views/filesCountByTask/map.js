function(doc) {
	if(doc.workflow){
	   if (doc.end_time && doc.state == 'new') emit([doc.workflow,doc.user, doc.state], 1);
           else     emit([doc.workflow,doc.user, doc.state], 1);

        }
}
