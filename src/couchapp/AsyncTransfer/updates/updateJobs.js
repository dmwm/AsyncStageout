function (doc,req) {
        doc.end_time = req.query.end_time;
        doc.state = req.query.state;
        return [doc, "OK"];
}
