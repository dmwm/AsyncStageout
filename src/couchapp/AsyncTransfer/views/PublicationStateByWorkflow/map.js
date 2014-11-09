fun({Doc}) ->
  Type = proplists:get_value(<<"type">>, Doc, null),
  case Type of
    <<"output">> ->
      Publish = proplists:get_value(<<"publish">>, Doc, null),
      case Publish of
        1 ->
          PubState = proplists:get_value(<<"publication_state">>, Doc, null),
          Workflow = proplists:get_value(<<"workflow">>, Doc, null),
          Emit(Workflow, PubState);
        _ -> ok; undefined -> ok; <<"">> -> ok; null -> ok end;
    _ -> ok; undefined -> ok; <<"">> -> ok; null -> ok end end.
