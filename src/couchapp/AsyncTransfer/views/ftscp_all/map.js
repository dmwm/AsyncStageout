fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    <<"new">> ->
      Lfn = proplists:get_value(<<"lfn">>, Doc, null),
      case Lfn of
         _ -> 
           User = proplists:get_value(<<"user">>, Doc, null),
           Group = proplists:get_value(<<"group">>, Doc, null),
           Role = proplists:get_value(<<"role">>, Doc, null),
           Dest = proplists:get_value(<<"destination">>, Doc, null),
           Source = proplists:get_value(<<"source">>, Doc, null),
           Emit([User, Group, Role, Dest, Source], Lfn);
        undefined -> ok;
        <<"">> -> ok;
        null -> ok
      end;
    undefined -> ok;
    <<"">> -> ok;
    _ -> ok;
    null -> ok
  end
end.
