function(doc) {

        if (doc.publication_state != 'published' && doc.publication_state != 'publication_failed' && doc.state == 'done' && doc.lfn && doc.publish_dbs_url) {
                var lfn = doc.lfn.replace('store', 'store/temp')
                var publish = 0
                if (doc.publish == 1) {
                        publish = doc.publish
                }
                if (doc.type != 'log'){
                        if (publish == 1){
                                var dbs_url = doc.dbs_url
                                var publish_dbs_url = doc.publish_dbs_url
                                        emit([doc.user, doc.group, doc.role, doc.dn, doc.workflow], [doc.destination, lfn, publish_dbs_url, doc.inputdataset, dbs_url, doc.job_end_time]);
                        }
                }
        }
}

