function (key, values, rereduce) {
  var output = {'total': 0, 'done': 0, 'failed': 0, 'new': 0, 'acquired':0,'resubmitted':0 ,  'killed': 0};

  if (rereduce) {
    for (var someValue in values) {
      output['total'] += values[someValue]['total'];
      output['new'] += values[someValue]['new'];
      output['acquired'] += values[someValue]['acquired'];
      output['resubmitted'] += values[someValue]['resubmitted'];
      output['done'] += values[someValue]['done'];
      output['failed'] += values[someValue]['failed'];
      output['killed'] += values[someValue]['killed'];
    }
  } else {
    for (var someValue in values) {
	if ((values[someValue]['state'] == 'failed')||(values[someValue]['state'] == 'done')|| (values[someValue]['state'] == 'killed')||(values[someValue]['state'] == 'resubmitted') || (values[someValue]['state'] == 'new') || (values[someValue]['state'] == 'acquired') ){
      		output[values[someValue]['state']] += 1;
        }
    output['total'] += 1;
    }
  }
return output;
}
