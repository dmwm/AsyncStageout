function (doc,req) {
        doc.db_update = parseFloat(req.query.db_update);
        return [doc, "OK"];
}
