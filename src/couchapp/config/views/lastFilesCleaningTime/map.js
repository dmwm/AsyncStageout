function(doc) {
    if (doc.last_cleaning_time){
                emit(doc.last_cleaning_time);
	}
}
