function (key, values, rereduce) {
  var output = {'total': 0, 'done': 0, 'failed': 0, 'other': 0};

  if (rereduce) {
    for (var someValue in values) {
      output['total'] += values[someValue]['total'];
      output['other'] += values[someValue]['other'];
      output['done'] += values[someValue]['done'];
      output['failed'] += values[someValue]['failed'];
    }
  } else {
    for (var someValue in values) {
	if ((values[someValue]['state'] == 'failed')||(values[someValue]['state'] == 'done')){
      		output[values[someValue]['state']] += 1;
        }else{
		output['other'] += 1;
	}
    output['total'] += 1;
    }
  }
return output;
}