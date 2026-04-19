import pytest

from pysqlscribe.aggregate_functions import avg
from pysqlscribe.cte import With, with_
from pysqlscribe.exceptions import DuplicateCTENameError, EmptyCTEError
from pysqlscribe.query import Query


def test_basic_cte():
    query_builder = Query("mysql")
    query_builder.select("department", avg("salary")).from_("employees").group_by(
        "department"
    )
    cte_query = (
        With("AvgSalaryByDepartment", dialect="mysql")
        .as_(query_builder)
        .select("*")
        .from_("AvgSalaryByDepartment")
        .build()
    )
    assert (
        cte_query
        == "WITH AvgSalaryByDepartment AS (SELECT `department`, AVG(salary) FROM `employees` GROUP BY `department`) SELECT * FROM `AvgSalaryByDepartment`"
    )


def test_multi_cte():
    user_query_builder = Query("mysql")
    user_query_builder.select("name", "id").from_("users").where("city = 'New York'")
    vehicle_query_builder = Query("mysql")
    vehicle_query_builder.select("vehicle_type", "vehicle_id", "owner_id").from_(
        "vehicles"
    ).where("city = 'New York'")
    cte_query = (
        With("UsersNY", dialect="mysql")
        .as_(user_query_builder)
        .with_("VehiclesNY")
        .as_(vehicle_query_builder)
        .select("name", "vehicle_type")
        .from_("UsersNY")
        .join("VehiclesNY", condition="id = owner_id")
        .build()
    )
    assert (
        cte_query
        == "WITH UsersNY AS (SELECT `name`, `id` FROM `users` WHERE city = 'New York'), VehiclesNY AS (SELECT `vehicle_type`, `vehicle_id`, `owner_id` FROM `vehicles` WHERE city = 'New York') SELECT `name`, `vehicle_type` FROM `UsersNY` INNER JOIN `VehiclesNY` ON id = owner_id"
    )


def test_cte_function():
    query_builder = Query("mysql")
    query_builder.select("department", avg("salary")).from_("employees").group_by(
        "department"
    )
    cte_query = (
        with_("AvgSalaryByDepartment", dialect="mysql")
        .as_(query_builder)
        .select("*")
        .from_("AvgSalaryByDepartment")
        .build()
    )
    assert (
        cte_query
        == "WITH AvgSalaryByDepartment AS (SELECT `department`, AVG(salary) FROM `employees` GROUP BY `department`) SELECT * FROM `AvgSalaryByDepartment`"
    )


def test_duplicate_cte_fails():
    query_builder = Query("mysql")
    query_builder.select("department", avg("salary")).from_("employees").group_by(
        "department"
    )
    with pytest.raises(DuplicateCTENameError):
        With("AvgSalaryByDepartment", dialect="postgres").as_(query_builder).with_(
            "AvgSalaryByDepartment"
        ).as_(query_builder).select("*").build()


def test_empty_cte_fails():
    with pytest.raises(EmptyCTEError):
        With("AvgSalaryByDepartment", dialect="postgres").build()


def test_recursive_cte():
    from pysqlscribe.table import Table

    # Anchor: top-level employees whose manager_id IS NULL.
    employees = Table("employees", "employee_id", "name", "manager_id", dialect="mysql")
    anchor = employees.select("employee_id", "name", "manager_id", "1 AS level").where(
        employees.manager_id.is_null()
    )

    # Recursive: join employees (aliased e) with the CTE (aliased ep).
    e = Table("employees", "employee_id", "name", "manager_id", dialect="mysql").as_(
        "e"
    )
    ep = Table("EmployeePaths", "employee_id", "level", dialect="mysql").as_("ep")
    recursive = e.select(
        e.employee_id, e.name, e.manager_id, (ep.level + 1).as_("level")
    ).join(ep, condition=(e.manager_id == ep.employee_id))

    cte_query = (
        with_("EmployeePaths", dialect="mysql", recursive=True)
        .as_(anchor.union(recursive, all_=True))
        .select("*")
        .from_("EmployeePaths")
        .order_by("level")
        .build()
    )
    assert cte_query == (
        "WITH RECURSIVE EmployeePaths AS ("
        "SELECT `employee_id`, `name`, `manager_id`, 1 AS level "
        "FROM `employees` WHERE employees.manager_id IS NULL "
        "UNION ALL "
        "SELECT `employee_id`, `name`, `manager_id`, ep.level + 1 AS level "
        "FROM `employees` AS e "
        "INNER JOIN `EmployeePaths` AS ep ON e.manager_id = ep.employee_id"
        ") SELECT * FROM `EmployeePaths` ORDER BY `level`"
    )
