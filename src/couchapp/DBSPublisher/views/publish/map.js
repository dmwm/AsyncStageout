function(doc) {
        if (doc.publication_state != 'published' && doc.publication_state != 'publication_failed' && doc.state == 'done' && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.dn, doc.workflow], [doc.destination, doc.lfn, doc.publish_dbs_url, doc.inputdataset, doc.dbs_url, doc.job_end_time]);
	}
}
