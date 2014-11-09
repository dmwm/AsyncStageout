fun({Doc}) ->
  Workflow = proplists:get_value(<<"workflow">>, Doc, null),
  case Workflow of
    undefined -> ok;
    <<"">> -> ok;
    null -> ok;
    _ ->
      State = proplists:get_value(<<"state">>, Doc, null),
      Id = proplists:get_value(<<"_id">>, Doc, null),
      case State of
        <<"failed">> -> Emit(Workflow, Id);
        <<"killed">> -> Emit(Workflow, Id);
        undefined -> ok;
        <<"">> -> ok;
        null -> ok;
        _ -> ok
    end
  end
end.
