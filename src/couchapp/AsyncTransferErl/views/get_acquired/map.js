%function(doc) {
%        if (doc.state == 'acquired'&& doc.lfn) {
%		emit([doc.user, doc.group, doc.role, doc.destination, doc.source], doc.lfn);
%	}
%}
fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    undefined -> ok;
    <<"">> -> ok;
    null -> ok;
    <<"acquired">> ->
      Lfn = proplists:get_value(<<"lfn">>, Doc, null),
      case Lfn of
        undefined -> ok;
        <<"">> -> ok;
        null -> ok;
         _ -> 
           User = proplists:get_value(<<"user">>, Doc, null),
           Group = proplists:get_value(<<"group">>, Doc, null),
           Role = proplists:get_value(<<"role">>, Doc, null),
           Dest = proplists:get_value(<<"destination">>, Doc, null),
           Source = proplists:get_value(<<"source">>, Doc, null),
           Emit([User, Group, Role, Dest, Source], Lfn)
      end;
    _ -> ok
  end
end.
