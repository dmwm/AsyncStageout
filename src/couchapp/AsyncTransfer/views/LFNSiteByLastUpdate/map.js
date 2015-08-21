function(doc) {
	if(doc.source_lfn && doc.state == 'done'){
		emit(doc.last_update, {"lfn": doc.source_lfn, "location": doc.source});
	}
}
