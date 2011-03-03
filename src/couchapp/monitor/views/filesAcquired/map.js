function(doc) {

  		if (doc.state == 'acquired') {
  			emit([doc.user, doc.task, doc.source, doc.destination], doc._id);
  			}
}