defmodule QueryTest do
  use ExUnit.Case
  doctest Rexdb

  alias Rexdb.Query

  describe "#compile/1" do
    test "will produce a predicate" do
      compiled =
        %Query{where: [eq: [:id, 1]]}
        |> Query.compile()

      assert compiled.where.(%{id: 1})
      refute compiled.where.(%{id: 0})
    end

    test "will produce a compound predicate" do
      compiled =
        %Query{where: [eq: [:id, 1], lt: [:age, 100]]}
        |> Query.compile()

      assert compiled.where.(%{id: 1, age: 99})
      refute compiled.where.(%{id: 0, age: 101})
    end
  end
end

defmodule TableTest do
  use ExUnit.Case

  alias Rexdb.Table

  @id_n 18_446_744_073_709_551_615

  describe "#insert/2" do
    test "will insert a row" do
      id = id()

      table =
        %Table{columns: [:id, :age]}
        |> Table.insert(id, %{age: 32_000})

      assert table.data[id][:age] == 32_000
    end
  end

  defp id do
    "#{:rand.uniform(@id_n)}"
    |> Base.encode64()
  end
end

defmodule DbTest do
  use ExUnit.Case

  alias Rexdb.Db

  describe "#create_table/3" do
    test "will result in a table" do
      db =
        %Db{}
        |> Db.create_table(:users, [:id, :age])

      assert db.tables[:users].columns == [:id, :age]
    end
  end
end

defmodule RexdbTest do
  use ExUnit.Case

  alias Rexdb.{Db, Query}

  setup [:setup_db]

  describe "#insert/2" do
    test "will insert rows", %{db: db} do
      {id, inserted} = db |> Rexdb.insert(:users, %{age: 3_000})

      assert inserted.tables[:users].data[id][:age] == 3_000
    end
  end

  describe "#query/2" do
    test "will return rows", %{db: db} do
      {id, inserted} = db |> Rexdb.insert(:users, %{age: 3_000})
      {_, inserted2} = inserted |> Rexdb.insert(:users, %{age: 1_999})

      query =
        %Query{select: [:id, :age], from: :users, where: [gt: [:age, 2_000]]}
        |> Query.compile()

      {:ok, [row]} = Rexdb.query(inserted2, query)

      assert row[:age] == 3_000
      assert row[:id] == id
    end
  end

  defp setup_db(_) do
    db =
      %Db{}
      |> Db.create_table(:users, [:id, :age])

    [db: db]
  end
end
