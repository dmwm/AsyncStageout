function (doc,req) {
        doc.last_checkstatus_time = parseFloat(req.query.last_checkstatus_time);
        return [doc, "OK"];
}
