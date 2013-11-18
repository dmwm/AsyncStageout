function(doc) {
        if (doc.type == "aso_file"){
                if (doc.state=='killed'){
                        emit(doc.jobid, doc.lfn);
                }

        }
}
