function(doc) {
        if (doc.publication_state == 'published') {
	        emit([doc.user, doc.group, doc.role, doc.workflow], doc.last_update);
        }
}
