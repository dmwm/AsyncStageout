function(doc) {
	if(doc.workflow){
		emit(doc.workflow, {'jobid': doc.jobid, 'start_time': doc.start_time, 'state': doc.state});
	}
}
