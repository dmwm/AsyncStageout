fun({Doc}) ->
  Publication_state = proplists:get_value(<<"publication_state">>, Doc, null),
  case Publication_state of
    <<"published">> -> ok; <<"publication_failed">> -> ok;
    _ ->
      State = proplists:get_value(<<"state">>, Doc, null),
      case State of
      <<"Done">> -> 
        Publish = proplists:get_value(<<"dbs_url">>, Doc, null),
        case Publish of
          1 -> 
            Type = proplists:get_value(<<"type">>, Doc, null),
            case Type of
              <<"log">> -> ok;
              _ -> 
                User = proplists:get_value(<<"user">>, Doc, null),
                Lfn = proplists:get_value(<<"lfn">>, Doc, null),
                Dbs = proplists:get_value(<<"dbs_url">>, Doc, null),
                Group = proplists:get_value(<<"group">>, Doc, null),
                Role = proplists:get_value(<<"role">>, Doc, null),
                Workflow = proplists:get_value(<<"workflow">>, Doc, null),
                Destination = proplists:get_value(<<"destination">>, Doc, null),
                InputDataset = proplists:get_value(<<"inputdataset">>, Doc, null),
                EndTime = proplists:get_value(<<"end_time">>, Doc, null), % Need to split!!!!!!
                Emit([User, Group, Role, Workflow], [Destination, Lfn, InputDataset, Dbs, EndTime]);
              undefined -> ok; <<"">> -> ok; null -> ok end;
          _ -> ok; undefined -> ok; <<"">> -> ok; null -> ok end;
      _ -> ok; undefined -> ok; <<"">> -> ok; null -> ok end;
    undefined -> ok; <<"">> -> ok; null -> ok end end.
