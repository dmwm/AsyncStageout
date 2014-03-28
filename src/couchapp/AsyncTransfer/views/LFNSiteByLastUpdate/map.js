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
			var final_lfn = doc.lfn
                	var lfn = final_lfn.replace('/store/user', '/store/temp/user')
		}
		emit(doc.last_update, {"lfn": lfn, "location": doc.source});
	}
}
