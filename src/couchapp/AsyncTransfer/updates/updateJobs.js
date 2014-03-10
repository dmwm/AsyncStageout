function (doc,req) {
        if (req.query.state == 'done'){
                doc.end_time = req.query.end_time;
                doc.lfn = req.query.lfn;
        }
        if (req.query.state == 'retry'){
                doc.failure_reason = req.query.failure_reason;
                doc.retry_count.push(req.query.retry);
        }
        if (req.query.state == 'failed'){
                doc.retry_count.push(req.query.retry);
                doc.failure_reason = req.query.failure_reason;
                doc.end_time = req.query.end_time;
        }
        if (req.query.state == 'killed'){
                doc.end_time = req.query.end_time;
                doc.retry_count.push(req.query.retry);
        }
	if (req.query.state == 'new'){
                if (req.query.retry != null){
                    doc.retry_count.push(req.query.retry);
                }
                else {
                    doc.retry_count = []
                }
                if (req.query.end_time != null){
                    doc.end_time = req.query.end_time;
                }
	}
	doc.last_update = parseInt(req.query.last_update)
        doc.state = req.query.state;
        return [doc, "OK"];

}
