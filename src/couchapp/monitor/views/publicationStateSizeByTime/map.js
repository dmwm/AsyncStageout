function(doc) {
    if(doc.end_time) {
        var start = doc.end_time;
        var day = start.split(' ')[0];
        var time = start.split(' ')[1];
        var yy = day.split('-')[0];
        var mm = day.split('-')[1];
        var dd = day.split('-')[2];
        var h = time.split(':')[0];
        var m = time.split(':')[1];
        var s = time.split(':')[2].split('.')[0];
        var startDate = new Date(yy, mm, dd, h, m, s);
        yy =  startDate.getUTCFullYear();
        mm = startDate.getUTCMonth();
        dd = startDate.getUTCDate();
        h = startDate.getUTCHours();
        m = startDate.getUTCMinutes();
        s = startDate.getUTCSeconds();
	emit([yy, mm, dd, h, m, s], {"publication_state": doc.publication_state, "size": doc.size});		
    }
}
