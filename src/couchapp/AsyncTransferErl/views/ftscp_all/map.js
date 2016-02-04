%function(doc) {
%            if (doc.state == 'new' && doc.lfn) {
%                        emit([doc.user, doc.group, doc.role, doc.destination, doc.source], [doc.source_lfn, doc.destination_lfn]);
%                            }
%}
fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    undefined -> ok;
    <<"">> -> ok;
    null -> ok;
    <<"new">> ->
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
          SourceLFN = proplists:get_value(<<"source_lfn">>, Doc, null),
          DestLFN = proplists:get_value(<<"destination_lfn">>, Doc, null),
          Emit([User, Group, Role, Dest, Source], [SourceLFN, DestLFN])
      end;
    _ -> ok
  end
end.
