function(doc) {
	if(doc.workflow){
		emit(doc.workflow, {'jobid': doc.jobid, 'state': doc.state});
	}
}
