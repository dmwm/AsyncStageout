function(doc) {
  emit(doc.source, 1);
  emit(doc.destination, 1);
}