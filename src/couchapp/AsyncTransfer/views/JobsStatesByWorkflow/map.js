fun({Doc}) ->
  Workflow = proplists:get_value(<<"workflow">>, Doc, null),
  case Workflow of
    _ ->
      State = proplists:get_value(<<"state">>, Doc, null),
      Emit(Workflow, State);
    undefined -> ok;
    <<"">> -> ok;
    null -> ok
  end
end.
