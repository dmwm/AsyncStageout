function complete_job(doc, req) {
	if ( doc['state'] != 'done' ) {
        	return false;
        }
        return true;
}

function(doc) {
	if(doc.lfn && complete_job(doc)){
                if (doc.lfn.indexOf("temp") > 0) {
		        var lfn = doc.lfn
                }
                else {
                	var lfn = doc.lfn.replace('/store', '/store/temp')
		}
		emit(doc.last_update, {"lfn": lfn, "location": doc.source});
	}
}
