function(doc) {
	if(doc.user){
		if (doc.end_time && doc.state=='new') emit([doc.user], {"state": 'resubmitted'});
		else emit([doc.user], {"state": doc.state});
	}
}
