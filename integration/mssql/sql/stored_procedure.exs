defmodule Ecto.Integration.StoredProcedureTest do
  use Ecto.Integration.Case, async: true
  @moduletag :integration

  alias Ecto.Integration.TestRepo
  alias Mssqlex.Result

  test "return a resultset from a select" do
    {:ok, %Result{}} = TestRepo.query(
      "CREATE TABLE dbo.simple_select (name nvarchar(50));")
    {:ok, %{num_rows: 2}} = TestRepo.query(
      ["INSERT INTO dbo.simple_select (name) VALUES ('Steven'), ('Bob');"])

    {:ok, %Result{}} = TestRepo.query("""
      CREATE PROCEDURE dbo.select_all AS 
      SELECT  * 
      FROM    sp_test.dbo.simple_select
      ORDER BY name DESC
    """)

    assert {:ok, %Result{columns: ["name"], num_rows: 2, rows: [["Steven"], ["Bob"]]}}
      = TestRepo.query("EXEC select_all;")
  end

  test "return multiple resultsets from a select" do
    {:ok, %Result{}} = TestRepo.query("""
      CREATE PROCEDURE dbo.select_three AS 
      SELECT * FROM sp_test.dbo.simple_select WHERE name = 'Steven'
      SELECT * FROM sp_test.dbo.simple_select WHERE name = 'Bob'
      SELECT * FROM sp_test.dbo.simple_select ORDER BY name ASC
    """, [])

    assert {:ok, [%Result{columns: ["name"], num_rows: 1, rows: [["Steven"]]},
                  %Result{columns: ["name"], num_rows: 1, rows: [["Bob"]]},
                  %Result{columns: ["name"], num_rows: 2, rows: [["Bob"], ["Steven"]]}
                 ]}
      == TestRepo.query("EXEC select_three;", [], parameterized: false)
  end

  test "handle input/output parameters" do
    {:ok, %Result{}} = TestRepo.query("""
      CREATE PROCEDURE dbo.input_ouput_params 
        @p_in NUMERIC(10, 0),
        @p_out NUMERIC(10, 0) OUTPUT,
        @p_in_out VARCHAR(50) OUTPUT
      AS 
        SET @p_out = @p_in * 2
        SET @p_in_out = UPPER(@p_in_out)
    """, [])
    assert {:ok, %Result{columns: [], num_rows: 1, rows: [[6, "TEST"]]}}
      = TestRepo.query("EXEC input_ouput_params ?1, ?2, ?3", [in: 3, out: 0, inout: "test"])
  end

  test "return nothing" do
    {:ok, %Result{}} = TestRepo.query("""
      CREATE PROCEDURE dbo.return_nothing 
      AS 
        return
    """, [])
    assert {:ok, %Result{columns: nil, num_rows: :undefined, rows: nil}}
      = TestRepo.query("EXEC return_nothing", [])
  end


end
