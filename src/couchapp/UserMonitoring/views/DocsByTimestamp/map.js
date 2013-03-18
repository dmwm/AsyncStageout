function(doc) {
        if (doc.type == "aso_workflow") {
                emit(doc.last_update, doc._id);
        }
	else if (doc.type == "aso_file" && (doc.state == "failed" || doc.state == "done")) {
		emit(doc.timestamp, doc._id);
	}           
}

