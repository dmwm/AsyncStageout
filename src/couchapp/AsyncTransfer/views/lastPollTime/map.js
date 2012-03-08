function(doc) {
        if (doc.lfn) {
                emit(doc.dbSource_update, doc._id) ;
        }
}
