/*
 * Query with group=true and differing group_level for various granularity
 * group_level=1 : per server
 * group_level=2 : per year
 * group_level=3 : per month
 * group_level=4 : per week
 * group_level=5 : (equiv to not setting group level) per day
 */
function(doc) {
  var k = [doc.fts];
  k.push.apply(k, doc.day.split("-"));
	k.splice(3, 0 ,Math.ceil(k[3]/7));
  emit(k, doc.timing.min_transfer_duration);
  emit(k, doc.timing.max_transfer_duration);
};