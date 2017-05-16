function(doc) {
        if (doc.lfn){
                emit(doc.source, 1);
                emit(doc.destination, 1);
                }
        }
