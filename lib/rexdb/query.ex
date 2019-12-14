defmodule Rexdb.Query do
  defstruct [:select, :from, :where, :join, _compiled: false]

  @type clause :: {atom(), list()}
  @type query_predicate :: (map -> boolean)
  @type uncompiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          where: [clause],
          _compiled: false
        }
  @type compiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          where: query_predicate(),
          _compiled: true
        }
  @type t :: compiled() | uncompiled()

  @spec compile(uncompiled()) :: compiled()
  def compile(%__MODULE__{where: where} = q),
    do: %{q | where: compile_clause(where), _compiled: true}

  @spec compile_clause(clause | [clause]) :: query_predicate()
  defp compile_clause(wheres) when is_list(wheres) do
    Enum.reduce(wheres, fn _ -> true end, fn clause, pred ->
      pred1 = compile_clause(clause)

      fn row ->
        pred.(row) && pred1.(row)
      end
    end)
  end

  defp compile_clause({:eq, [field, value]}), do: do_compile(&==/2, field, value)
  defp compile_clause({:lt, [field, value]}), do: do_compile(&</2, field, value)
  defp compile_clause({:lte, [field, value]}), do: do_compile(&<=/2, field, value)
  defp compile_clause({:gt, [field, value]}), do: do_compile(&>/2, field, value)
  defp compile_clause({:gte, [field, value]}), do: do_compile(&>=/2, field, value)

  defp do_compile(pred, field, value) do
    fn row ->
      Map.get(row, field)
      |> pred.(value)
    end
  end
end
