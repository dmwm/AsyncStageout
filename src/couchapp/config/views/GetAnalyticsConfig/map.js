function(doc) {
        if (doc.summaries_expiration_days) {
        	emit(doc.summaries_expiration_days, 1);
        }
}
