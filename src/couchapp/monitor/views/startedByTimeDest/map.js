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

  yy =  startDate.getUTCFullYear();
  mm = startDate.getUTCMonth();
  dd = startDate.getUTCDate();
  h = startDate.getUTCHours();
  m = startDate.getUTCMinutes();
  s = startDate.getUTCSeconds();

  emit([doc.destination, yy, mm, dd, h, m, s], {"state": doc.state});
}
}