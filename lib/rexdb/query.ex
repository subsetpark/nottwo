defmodule Rexdb.Query do
  defstruct [:select, :from, :where, :join]

  def compile(wheres) when is_list(wheres) do
    Enum.reduce(wheres, fn _ -> true end, fn clause, pred ->
      pred1 = compile(clause)
      fn row ->
        pred.(row) && pred1.(row)
      end
    end)
  end

  def compile({:eq, [field, value]}) do
    fn row ->
      Map.get(row, field) == value
    end
  end

  def compile(%__MODULE__{where: where} = q), do: %{q | where: compile(where)}
end
