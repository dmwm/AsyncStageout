function(doc) {
        if (doc.state == 'running' && doc.countries) {
                for (c in doc.countries) {
                        emit(doc.countries[c], doc.url);
                }
        }
}
