function(doc) {
  if (doc.fts) {
    emit([doc.fts, doc.day], doc.sites_served);
  }
};
