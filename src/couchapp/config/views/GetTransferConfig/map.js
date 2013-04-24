function(doc) {
        if (doc.scheduling_algo) {
        	emit([doc.pool_size, doc.max_files_per_transfer, doc.scheduling_algo], 1);
        }
}
