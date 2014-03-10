function(doc) {
        if (doc.lfn.indexOf("temp") > 0) {
		var lfn = doc.lfn
        }
        else {
		var temp_lfn = doc.lfn
                var user_dir = temp_lfn.split('/')[3]
                var username = user_dir.split('.')[0]
                var lfn = temp_lfn.replace(user_dir, username)
	}
	emit(doc.workflow, {"lfn": lfn, "jobid": doc.jobid, "source": doc.source, "destination": doc.destination, "checksums": doc.checksums, "size": doc.size, "type": doc.type});
}
