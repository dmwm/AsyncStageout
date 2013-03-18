function(doc) {
  if((doc.state)&&(doc.start_time)&&(doc.state != 'done')&&(doc.state != 'failed')){
  var start = doc.start_time;
  var day = start.split(' ')[0];
  var time = start.split(' ')[1];

  var yy = day.split('-')[0];
  var mm = day.split('-')[1];
  var dd = day.split('-')[2];

  var h = time.split(':')[0];
  var m = time.split(':')[1];
  var s = time.split(':')[2].split('.')[0];

  var startDate = new Date(yy, mm, dd, h, m, s);

                yy_utc =  startDate.getUTCFullYear();
                mm_utc = startDate.getUTCMonth();
                dd_utc = startDate.getUTCDate();
                h_utc = startDate.getUTCHours();
                m_utc = startDate.getUTCMinutes();
                s_utc = startDate.getUTCSeconds();

                if ( mm == "12"){
                        emit([parseInt(yy), parseInt(mm), parseInt(dd), parseInt(h), parseInt(m), parseInt(s)], {"state": doc.state});
                }
                else {
                        emit([yy_utc, mm_utc, dd_utc, h_utc, m_utc, s_utc], {"state": doc.state});

                }

}
}
