function(doc) {
	if(doc.state == 'failed'){
		emit([doc.destination, doc.source, doc.user, doc.workflow, doc.end_time, doc.failure_reason], doc.lfn);
	}
}
