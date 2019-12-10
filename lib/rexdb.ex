defmodule Rexdb do
  alias Rexdb.{Db, Table, Query}

  def insert(%Db{tables: tables} = db, table_name, attrs) do
    id = :crypto.strong_rand_bytes(16)
    tables = update_in(tables, [table_name], &Rexdb.Table.insert(&1, id, attrs))
    %{db | tables: tables}
  end

  def query(%Db{tables: tables}, %Query{select: select, from: from, where: where}) do
    tables
    |> Map.get(from)
    |> Map.get(:data)
    |> Enum.filter(fn {_, row} -> where.(row) end)
    |> Enum.map(fn {_, row} -> Map.take(row, select) end)
  end
end
