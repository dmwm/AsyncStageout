fun({Doc}) ->
  Workflow = proplists:get_value(<<"workflow">>, Doc, null),
  case Workflow of
    _ ->
      JobId = proplists:get_value(<<"jobid">>, Doc, null),
      State = proplists:get_value(<<"state">>, Doc, null),
      Emit(Workflow, {[{<<"jobid">>, JobId},{<<"state">>, State}]});
    undefined -> ok;
    <<"">> -> ok;
    null -> ok
  end
end.
