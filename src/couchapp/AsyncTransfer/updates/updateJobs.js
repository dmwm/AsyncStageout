function (doc,req) {
        doc.retry_count = req.query.retry_count;
        doc.end_time = req.query.end_time;
        doc.state = req.query.state;
        return [doc, "OK"];
}
