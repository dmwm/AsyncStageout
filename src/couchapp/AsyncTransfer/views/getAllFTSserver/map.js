function(doc) {
        if (doc.countries) {
                for (c in doc.countries) {
                        emit(doc.countries[c], doc.url);
                }
        }
}
