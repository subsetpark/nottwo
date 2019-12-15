defmodule QueryTest do
  use ExUnit.Case
  doctest Rexdb

  alias Rexdb.Query

  describe "#compile/1" do
    test "will produce a predicate" do
      compiled =
        %Query{where: [eq: [:id, 1]]}
        |> Query.compile()

      assert compiled._pred.({:ok, %{id: 1}})
      refute compiled._pred.({:ok, %{id: 0}})
    end

    test "will produce a compound predicate" do
      compiled =
        %Query{where: [eq: [:id, 1], lt: [:age, 100]]}
        |> Query.compile()

      assert compiled._pred.({:ok, %{id: 1, age: 99}})
      refute compiled._pred.({:ok, %{id: 0, age: 101}})
    end

    test "will join on other tables" do
      compiled =
        %Query{
          select: [:name],
          from: :teams,
          join: [users: :user_id]
        }
        |> Query.compile()

      {user_id, db} =
        %Rexdb.Db{}
        |> Rexdb.Db.create_table(:users, [:id, :age])
        |> Rexdb.insert(:users, %{age: 3_000})

      row = %{name: "falcons", user_id: user_id}

      got = compiled._getter.(row, db)

      assert got == %{name: "falcons", user_id: user_id, "users.age": 3_000, "users.id": user_id}
    end

    test "will join on multiple tables" do
      compiled =
        %Query{
          select: [:name],
          from: :teams,
          join: [users: :user_id, flags: :flag_id]
        }
        |> Query.compile()

      {user_id, db} =
        %Rexdb.Db{}
        |> Rexdb.Db.create_table(:users, [:id])
        |> Rexdb.Db.create_table(:flags, [:id])
        |> Rexdb.insert(:users, %{})

      {flag_id, db} = Rexdb.insert(db, :flags, %{})

      row = %{user_id: user_id, flag_id: flag_id}

      got = compiled._getter.(row, db)

      assert got == %{
               user_id: user_id,
               "users.id": user_id,
               flag_id: flag_id,
               "flags.id": flag_id
             }
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

      row =
        table
        |> Table.retrieve(id)

      assert row[:age] == 32_000
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

  alias Rexdb.{Db, Query, Table}

  setup [:setup_db]

  describe "#insert/2" do
    test "will insert rows", %{db: db} do
      {id, inserted} = db |> Rexdb.insert(:users, %{age: 3_000})

      row = Table.retrieve(inserted.tables[:users], id)
      assert row[:age] == 3_000
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

    test "will join on other tables" do
      compiled =
        %Query{
          select: [:name],
          from: :teams,
          join: [users: :user_id],
          where: [gt: [:"users.age", 2_000]]
        }
        |> Query.compile()

      {user_id, db} =
        %Rexdb.Db{}
        |> Rexdb.Db.create_table(:users, [:id, :age])
        |> Rexdb.Db.create_table(:teams, [:id, :name])
        |> Rexdb.insert(:users, %{age: 3_000})

      {_, db} = Rexdb.insert(db, :teams, %{user_id: user_id, name: "falcons"})
      {_, db} = Rexdb.insert(db, :teams, %{name: "penguins"})

      {:ok, [row]} = Rexdb.query(db, compiled)

      assert %{name: "falcons"} == row
    end
  end

  defp setup_db(_) do
    db =
      %Db{}
      |> Db.create_table(:users, [:id, :age])

    [db: db]
  end
end
