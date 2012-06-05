function complete_job(doc, req) {
        if ( doc['state'] != 'done' ) {
                return false;
        }
        return true;
}

function(doc) {
	if(doc.lfn && complete_job(doc)){
		emit(doc.last_update, {"lfn": doc.lfn, "location": doc.source});
	}
}
