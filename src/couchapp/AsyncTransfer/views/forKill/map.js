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
        undefined -> ok;
        <<"">> -> ok;
        null -> ok;
        <<"new">> -> Emit(Workflow, Id);
        <<"acquired">> -> Emit(Workflow, Id);
        <<"retry">> -> Emit(Workflow, Id);
        _ -> ok
    end
  end
end.
