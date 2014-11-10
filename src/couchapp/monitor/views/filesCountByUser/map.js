function(doc) {
	if(doc.user){
		if (doc.end_time && doc.state=='new') emit([doc.user, 'resubmitted'], 1);
		else emit([doc.user, doc.state], 1);
	}
}
