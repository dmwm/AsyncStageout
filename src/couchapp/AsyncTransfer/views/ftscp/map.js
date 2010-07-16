function(doc) {
  emit([doc.user, doc.destination, doc.source], doc._id);
}