defmodule Rexdb do
  alias Rexdb.{Db, Query}

  def insert(%Db{tables: tables} = db, table_name, attrs) do
    id = :crypto.strong_rand_bytes(16)
    tables = update_in(tables, [table_name], &Rexdb.Table.insert(&1, id, attrs))
    {id, %{db | tables: tables}}
  end

  @spec query(Db.t(), Query.t()) :: {:ok, [map()]} | {:err, atom}
  def query(%Db{}, %Query{_compiled: false}), do: {:err, :uncompiled_query}

  def query(%Db{tables: tables}, %Query{select: select, from: from, where: where, _compiled: true}) do
    rows =
      tables
      |> Map.get(from)
      |> Map.get(:data)
      |> Enum.filter(fn {_, row} -> where.(row) end)
      |> Enum.map(fn {_, row} -> Map.take(row, select) end)

    {:ok, rows}
  end
end
