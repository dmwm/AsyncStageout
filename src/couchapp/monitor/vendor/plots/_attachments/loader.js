function couchapp_load(scripts) {
  for (var i=0; i < scripts.length; i++) {
    document.write('<script src="'+scripts[i]+'"><\/script>')
  };
  document.write('<link href="vendor\/protovis\/tipsy\/tipsy.css" type="text\/css" rel="stylesheet"\/>')
};

couchapp_load([
  "vendor/plots/plots.js"
]);
