fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    <<"done">> ->
      Lfn = proplists:get_value(<<"lfn">>, Doc, null),
      case Lfn of
        undefined -> ok;
        <<"">> -> ok;
        null -> ok;
        _ ->
          Nlfn = re:replace(Lfn,"/store/user","/store/temp/user",[{return,list}]),
          Last_update = proplists:get_value(<<"last_update">>, Doc, null),
          Source = proplists:get_value(<<"source">>, Doc, null),
          Emit(Last_update, {[{<<"lfn">>, Nlfn},{<<"location">>, Source}]})
      end;
    _ -> ok;
    undefined -> ok;
    <<"">> -> ok;
    null -> ok
  end
end.
