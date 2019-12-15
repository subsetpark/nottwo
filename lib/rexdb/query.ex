defmodule Rexdb.Join do
  @type clause :: {atom(), atom()}
  @type t :: (map(), map() -> map)

  @spec id :: t()
  def id, do: fn row, _ -> row end

  @spec compile(clause) :: t()
  def compile({table, on}) do
    fn row, db ->
      with %{data: data} <- db.tables[table],
           {_, join_row} <- Enum.find(data, fn {_, join_row} -> row[on] == join_row[:id] end) do
        Enum.into(join_row, row, fn {key, value} -> {:"#{table}.#{key}", value} end)
      else
        _ -> row
      end
    end
  end

  @spec compose(t(), t()) :: t()
  def compose(g, f), do: &(f.(&1, &2) |> g.(&2))
end

defmodule Rexdb.Where do
  @type clause :: {atom(), list()}
  @type t :: (map -> boolean)

  @spec id :: t()
  def id, do: fn _ -> true end

  @spec compile(clause) :: t()
  def compile({:eq, [field, value]}), do: where(&==/2, field, value)
  def compile({:lt, [field, value]}), do: where(&</2, field, value)
  def compile({:lte, [field, value]}), do: where(&<=/2, field, value)
  def compile({:gt, [field, value]}), do: where(&>/2, field, value)
  def compile({:gte, [field, value]}), do: where(&>=/2, field, value)

  @spec compose(t(), t()) :: t()
  def compose(g, f), do: &(f.(&1) && g.(&1))

  defp where(pred, field, value) do
    fn row ->
      row_value = Map.get(row, field)
      # TODO: Update to allow IS NULL
      not is_nil(row_value) && pred.(row_value, value)
    end
  end
end

defmodule Rexdb.Query do
  alias Rexdb.{Join, Where}

  @derive {Inspect, only: [:select, :from, :where, :join]}
  defstruct [:select, :from, where: [], join: [], _getter: nil, _pred: nil, _compiled: false]

  @type clause :: Join.clause() | Where.clause()

  @type compiled_fn :: Join.t() | Where.t()

  @type uncompiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          join: [Join.clause()],
          where: [Where.clause()],
          _compiled: false
        }
  @type compiled :: %__MODULE__{
          select: [atom()],
          from: atom(),
          _getter: Join.t(),
          _pred: Where.t(),
          _compiled: true
        }
  @type t :: compiled() | uncompiled()

  @spec compile(uncompiled()) :: compiled()
  def compile(%__MODULE__{where: where, join: join} = q) do
    q
    |> Map.put(:_getter, compile(join, Join))
    |> Map.put(:_pred, compile(where, Where))
    |> Map.put(:_compiled, true)
  end

  @spec compile([clause], Join | Where) :: compiled_fn()
  defp compile(clauses, type) when is_list(clauses) do
    clauses
    |> Enum.reduce(type.id(), fn clause, f ->
      clause
      |> type.compile()
      |> type.compose(f)
    end)
  end
end
