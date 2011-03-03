function(doc) {

  		if (doc.state == 'done') {
  			emit([doc.user, doc.task, doc.source, doc.destination], doc._id);
  			}
}