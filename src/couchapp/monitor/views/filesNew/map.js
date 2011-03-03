function(doc) {

  		if (doc.state == 'new') {
  			emit([doc.user, doc.task, doc.source, doc.destination], doc._id);
  			}
}