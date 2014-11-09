fun({Doc}) ->
  Workflow = proplists:get_value(<<"workflow">>, Doc, null),
  case Workflow of
    _ -> 
      Publication_state = proplists:get_value(<<"publication_state">>, Doc, null),
      case Publication_state of
        <<"pulication_failed">> -> 
          Id = proplists:get_value(<<"_id">>, Doc, null),
          Emit(Workflow, Id);
        _ -> ok; undefined -> ok; <<"">> -> ok; null -> ok end;
    undefined -> ok; <<"">> -> ok; null -> ok end end.
