function(doc) {
	if (doc.workflow && (doc.state=='new' || doc.state=='acquired' || doc.state=='retry')){
		emit([doc.workflow, fts_jobid], doc.fts_fileid);
	}
}
