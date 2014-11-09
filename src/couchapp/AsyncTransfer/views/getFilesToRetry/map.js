fun({Doc}) ->
  State = proplists:get_value(<<"state">>, Doc, null),
  case State of
    <<"retry">> ->
      Lfn = proplists:get_value(<<"lfn">>, Doc, null),
      case Lfn of
        _ ->   
          Id = proplists:get_value(<<"_id">>, Doc, null),
          LastUpdate = proplists:get_value(<<"last_update">>, Doc, null),
          Emit(Id, LastUpdate);
        undefined -> ok;
        null -> ok;
        <<"">> -> ok
      end;
    <<"">> -> ok;
    undefined -> ok;
    null -> ok;
    _ -> ok
  end
end.
