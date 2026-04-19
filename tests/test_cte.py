import pytest

from pysqlscribe.aggregate_functions import avg
from pysqlscribe.cte import With, with_
from pysqlscribe.exceptions import DuplicateCTENameException, EmptyCTEException
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
    with pytest.raises(DuplicateCTENameException):
        With("AvgSalaryByDepartment", dialect="postgres").as_(query_builder).with_(
            "AvgSalaryByDepartment"
        ).as_(query_builder).select("*").build()


def test_empty_cte_fails():
    with pytest.raises(EmptyCTEException):
        With("AvgSalaryByDepartment", dialect="postgres").build()
