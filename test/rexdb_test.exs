defmodule RexdbTest do
  use ExUnit.Case
  doctest Rexdb

  test "greets the world" do
    assert Rexdb.hello() == :world
  end
end
