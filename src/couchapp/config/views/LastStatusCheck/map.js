function(doc) {
    if (doc.last_checkstatus_time){
                emit(doc.last_checkstatus_time);
	}
}
