function(doc) {
	if(doc.workflow){
		emit(doc.workflow, {'jobid': doc.jobid, 'job_retry_count': doc.job_retry_count, 'state': doc.state});
	}
}
