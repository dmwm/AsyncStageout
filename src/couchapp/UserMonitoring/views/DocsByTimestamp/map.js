function(doc) {
        if (doc.type == "summary_per_workflow") {
                emit(doc.last_update, doc._id);
        }
	else if (doc.type == "summary_per_file") {
		emit(doc.timestamp, doc._id);
	}           
}

