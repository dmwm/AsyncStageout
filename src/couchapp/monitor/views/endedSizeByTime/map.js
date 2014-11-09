fun({Doc}) ->
  EndTime = proplists:get_value(<<"end_time">>, Doc, null),
  case EndTime of
    null -> ok;
    <<"">> -> ok;
    undefined -> ok;
    _ -> 
      [Year, Month, Day, Hours, Min, Sec, Milli] = string:tokens(EndTime, "- :." ),
      State = proplists:get_value(<<"state">>, Doc, null),
      Size = proplists:get_value(<<"size">>, Doc, null),
      case State of
        undefined -> ok;
        null -> ok;
        <<"">> -> ok;
        <<"new">> -> 
          State = "resubmitted",
          Emit([Year, Month, Day, Hours, Min, Sec], {[{<<"state">>, State},{<<"size">>,Size}]});
        <<"retry">> ->
          State = "resubmitted",
          Emit([Year, Month, Day, Hours, Min, Sec], {[{<<"state">>, State},{<<"size">>,Size}]});
        _ -> 
          Emit([Year, Month, Day, Hours, Min, Sec], {[{<<"state">>, State},{<<"size">>,Size}]})
      end
  end
end.
