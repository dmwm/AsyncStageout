function(doc) {

        if (doc.publication_state != 'published' && doc.publication_state != 'publication_failed' && doc.state == 'done' && doc.lfn && doc.dbs_url && doc.rest_host && doc.rest_uri && doc.publish == 1) {
                                        emit( doc.user, [doc.rest_host, doc.rest_uri]);
        }
}
