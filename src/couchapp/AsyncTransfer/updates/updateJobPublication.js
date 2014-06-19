function (doc,req) {
        doc.last_update = parseInt(req.query.last_update)
        doc.publication_state = req.query.publication_state;
        return [doc, "OK"];

}
