function(doc) {
    if (doc.db_update){
                emit(doc.db_update);
	}
}
