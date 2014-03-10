function(doc) {
	emit(doc.workflow, {doc.lfn, doc.jobid, doc.source, doc.destination, doc.checksums, doc.size, doc.type});
}
