function(doc) {
        if (doc.state == 'down' && doc.countries) {
                for (c in doc.countries) {
                        emit(doc.countries[c], 1);
                }
        }
}
