from pysqlscribe.aggregate_functions import avg
from pysqlscribe.cte import With, with_
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
