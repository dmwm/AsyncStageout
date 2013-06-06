function (doc,req) {
        doc.last_cleaning_time = parseInt(req.query.last_cleaning_time);
        return [doc, "OK"];
}
