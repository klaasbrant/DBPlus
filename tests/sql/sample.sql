-- name: get_all_employees
-- Returns all employees
SELECT * FROM employees;

-- name: get_by_dept
-- Returns employees filtered by department
SELECT * FROM employees WHERE dept = :dept;
