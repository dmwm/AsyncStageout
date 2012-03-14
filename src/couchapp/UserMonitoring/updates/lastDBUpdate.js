function (doc,req) {
        doc.db_update = parseInt(req.query.db_update);
        return [doc, "OK"];
}
