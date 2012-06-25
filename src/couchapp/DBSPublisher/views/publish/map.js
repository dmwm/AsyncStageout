function(doc) {
        if (doc.publication_state != 'published' && doc.publication_state != 'publication_failed' && doc.state == 'done' && doc.lfn) {
		emit([doc.user, doc.group, doc.role, doc.dn, doc.workflow], [doc.destination, doc.lfn, doc.dbs_url_for_publication, doc.inputDataset, doc.dbs_url, doc.job_end_time]);
	}
}
