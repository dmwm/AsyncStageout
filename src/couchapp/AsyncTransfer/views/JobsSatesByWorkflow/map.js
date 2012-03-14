function(doc) {
	if(doc.workflow){
		emit(doc.workflow, doc.state);
	}
}
