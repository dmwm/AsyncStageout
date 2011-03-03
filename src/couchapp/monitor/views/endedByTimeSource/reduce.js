function(keys, values, rereduce) {
  var output = {'done': 0, 'failed': 0};
  if (rereduce) {
      for (var someValue in values) {
      output['done'] += values[someValue]['done'];
      output['failed'] += values[someValue]['failed'];
    }
  } else {
      for (var someValue in values) {
      	output[values[someValue]['state']] += 1;
	}
      
    }
  return output;
}