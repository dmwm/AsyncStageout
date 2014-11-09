fun({Doc}) ->
  Lfn = proplists:get_value(<<"lfn">>, Doc, null),
  case Lfn of
    _ -> 
      State = proplists:get_value(<<"state">>, Doc, null),
      case State of
        <<"done">> -> ok; <<"failed">> -> ok;
        _ -> 
           User = proplists:get_value(<<"user">>, Doc, null),
           Group = proplists:get_value(<<"group">>, Doc, null),
           Role = proplists:get_value(<<"role">>, Doc, null),
           Time = proplists:get_value(<<"start_time">>, Doc, null),
           Emit([User, Group, Role, Time], 1);
        undefined -> ok; <<"">> -> ok; null -> ok end;
    undefined -> ok; <<"">> -> ok; null -> ok end end.
