%function(doc) {
%	if (doc.state == 'retry' && doc.lfn) {
%		emit(doc._id, doc.last_update);
%	}
%}
fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    undefined -> ok;
    <<"">> -> ok;
    null -> ok;
    <<"retry">> ->
      Lfn = proplists:get_value(<<"lfn">>, Doc, null),
      case Lfn of
        undefined -> ok;
        <<"">> -> ok;
        null -> ok;
        _ ->   
          Id = proplists:get_value(<<"_id">>, Doc, null),
          LastUpdate = proplists:get_value(<<"last_update">>, Doc, null),
          Emit(Id, LastUpdate)
      end;
    _ -> ok
  end
end.
