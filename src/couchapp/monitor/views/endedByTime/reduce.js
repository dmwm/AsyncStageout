function(keys, values, rereduce) {
  var output = {'done': 0, 'failed': 0, 'killed': 0, 'resubmitted': 0};
  if (rereduce) {
      for (var someValue in values) {
      output['done'] += values[someValue]['done'];
      output['failed'] += values[someValue]['failed'];
      output['killed'] += values[someValue]['killed'];
      output['resubmitted'] += values[someValue]['resubmitted'];
      }
  } else {
      for (var someValue in values) {
        output[values[someValue]['state']] += 1;
        }

    }
  return output;
}

