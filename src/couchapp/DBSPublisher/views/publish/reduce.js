fun(Keys, Values, ReReduce) ->
  case ReReduce of
    _ -> length(Values);
    undefined -> length(Values);
    <<"">> -> length(Values);
    null -> length(Values)
  end
end.
