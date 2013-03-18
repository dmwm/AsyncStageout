function(doc) {
        if (doc.files_expiration_days) {
        	emit(doc.files_expiration_days, 1);
        }
}
