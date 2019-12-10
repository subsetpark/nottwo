defmodule Rexdb.Table do
  defstruct [data: %{}, columns: [:id]]

  def insert(%__MODULE__{data: data} = table, id, attrs) do
    attrs = Map.put(attrs, :id, id)

    %{table | data: Map.put(data, id, attrs)}
  end
end

defmodule Rexdb.Db do
  defstruct [tables: %{}]

  def create_table(%__MODULE__{tables: tables} = db, name, columns) do
    table = %Rexdb.Table{columns: columns}
    tables = Map.put(tables, name, table)
    %{db | tables: tables}
  end
end
