defmodule Nottwo.Table do
  defstruct data: :gb_trees.empty(), columns: [:id]

  @type t :: %__MODULE__{columns: [atom()], data: map()}

  def insert(%__MODULE__{data: data} = table, id, attrs) do
    attrs = Map.put(attrs, :id, id)

    %{table | data: :gb_trees.insert(id, attrs, data)}
  end

  def retrieve(%__MODULE__{data: data}, id) do
    :gb_trees.get(id, data)
  end

  def map(%__MODULE__{data: data} = t, fun) do
    data = :gb_trees.map(fun, data)
    %{t | data: data}
  end

  def filter(%__MODULE__{data: data} = t, fun) do
    data =
      Enum.reduce(t, data, fn {key, value}, acc ->
        if fun.({key, value}) do
          acc
        else
          :gb_trees.delete(key, acc)
        end
      end)
      |> :gb_trees.balance()

    %{t | data: data}
  end
end

defimpl Enumerable, for: Nottwo.Table do
  alias Nottwo.Table
  def count(%Table{data: tree}), do: {:ok, :gb_trees.size(tree)}
  def member?(%Table{data: tree}, key), do: {:ok, :gb_trees.is_defined(key, tree)}

  def slice(%Table{data: tree}) do
    fun = fn start, length ->
      start
      |> :gb_trees.iterator_from(tree)
      |> Enum.take(length)
    end

    {:ok, :gb_trees.size(tree), fun}
  end

  def reduce(%Table{data: tree}, acc, fun) do
    iter = :gb_trees.iterator(tree)

    reduce_iter(iter, acc, fun)
  end

  defp reduce_iter(iter, {:cont, acc}, fun) do
    :gb_trees.next(iter)
    |> case do
      {key, value, iter} ->
        reduce_iter(iter, fun.({key, value}, acc), fun)

      :none ->
        {:halted, acc}
    end
  end

  defp reduce_iter(_, {:halt, acc}, _fun), do: {:halted, acc}
end

defmodule Nottwo.Db do
  defstruct tables: %{}

  @type t :: %__MODULE__{tables: map()}

  def create_table(%__MODULE__{tables: tables} = db, name, columns) do
    table = %Nottwo.Table{columns: columns}
    tables = Map.put(tables, name, table)
    %{db | tables: tables}
  end
end
