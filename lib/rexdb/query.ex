defmodule Rexdb.Query do
  @derive {Inspect, only: [:select, :from, :where, :join]}
  defstruct [:select, :from, where: [], join: [], _getter: nil, _pred: nil, _compiled: false]

  @type clause :: {atom(), list()}
  @type join_on :: {atom(), atom()}

  @type query_predicate :: (map -> boolean)
  @type getter :: (map -> map)

  @type uncompiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          join: [join_on()],
          where: [clause],
          _compiled: false
        }
  @type compiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          _getter: getter(),
          _pred: query_predicate(),
          _compiled: true
        }
  @type t :: compiled() | uncompiled()

  @spec compile(uncompiled()) :: compiled()
  def compile(%__MODULE__{where: where, join: join} = q) do
    q
    |> Map.put(:_getter, compile_join(join))
    |> Map.put(:_pred, compile_where(where))
    |> Map.put(:_compiled, true)
  end

  @spec compile_join([join_on]) :: getter()
  defp compile_join(joins) when is_list(joins) do
    Enum.reduce(joins, fn row, _ -> row end, fn join, getter ->
      getter1 = compile_join_on(join)

      fn row, db ->
        getter.(row, db) |> getter1.(db)
      end
    end)
  end

  @spec compile_where([clause]) :: query_predicate()
  defp compile_where(wheres) when is_list(wheres) do
    Enum.reduce(wheres, fn _ -> true end, fn clause, pred ->
      pred1 = compile_clause(clause)

      fn row ->
        pred.(row) && pred1.(row)
      end
    end)
  end

  defp compile_join_on({table, on}) do
    fn row, db ->
      db.tables[table]
      |> Map.get(:data)
      |> Enum.find(fn {_, join_row} -> row[on] == join_row[:id] end)
      |> case do
        nil ->
          row

        {_, join_row} ->
          join_row
          |> Enum.into(%{}, fn {key, value} -> {:"#{table}.#{key}", value} end)
          |> Map.merge(row)
      end
    end
  end

  defp compile_clause({:eq, [field, value]}), do: do_compile(&==/2, field, value)
  defp compile_clause({:lt, [field, value]}), do: do_compile(&</2, field, value)
  defp compile_clause({:lte, [field, value]}), do: do_compile(&<=/2, field, value)
  defp compile_clause({:gt, [field, value]}), do: do_compile(&>/2, field, value)
  defp compile_clause({:gte, [field, value]}), do: do_compile(&>=/2, field, value)

  defp do_compile(pred, field, value) do
    fn row ->
      row_value = Map.get(row, field)
      # TODO: Update to allow IS NULL
      not is_nil(row_value) && pred.(row_value, value)
    end
  end
end
