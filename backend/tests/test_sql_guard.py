"""
Tests for the SQL Guard security module.
Run with: pytest tests/test_sql_guard.py -v
"""
import pytest
from app.security.sql_guard import SQLGuardError, validate_and_sanitize


# ── Valid queries ─────────────────────────────────────────────────────────────

class TestValidQueries:
    def test_simple_select(self):
        result = validate_and_sanitize("SELECT id, name FROM employees")
        assert result.is_valid
        assert "LIMIT" in result.sanitized_sql  # auto-injected

    def test_aggregate_no_limit_injected(self):
        sql = "SELECT SUM(salary) FROM employees"
        result = validate_and_sanitize(sql)
        assert result.is_valid
        assert "LIMIT" not in result.sanitized_sql

    def test_group_by_no_limit_injected(self):
        sql = "SELECT department_id, COUNT(*) FROM employees GROUP BY department_id"
        result = validate_and_sanitize(sql)
        assert result.is_valid
        assert "LIMIT" not in result.sanitized_sql

    def test_existing_limit_preserved(self):
        sql = "SELECT id FROM employees LIMIT 5"
        result = validate_and_sanitize(sql)
        assert result.is_valid
        # Should not have two LIMIT clauses
        assert result.sanitized_sql.upper().count("LIMIT") == 1

    def test_select_with_join(self):
        sql = (
            "SELECT e.name, d.name FROM employees e "
            "JOIN departments d ON e.department_id = d.id"
        )
        result = validate_and_sanitize(sql)
        assert result.is_valid

    def test_select_with_params(self):
        sql = "SELECT * FROM employees WHERE department_id = :dept_id"
        result = validate_and_sanitize(sql)
        assert result.is_valid


# ── Forbidden statements ──────────────────────────────────────────────────────

class TestForbiddenStatements:
    @pytest.mark.parametrize("sql", [
        "INSERT INTO employees (name) VALUES ('Hacker')",
        "UPDATE employees SET salary = 999999",
        "DELETE FROM employees WHERE id = 1",
        "DROP TABLE employees",
        "ALTER TABLE employees ADD COLUMN pwned TEXT",
        "TRUNCATE employees",
        "CREATE TABLE evil (id INT)",
    ])
    def test_forbidden_keywords(self, sql: str):
        with pytest.raises(SQLGuardError):
            validate_and_sanitize(sql)

    def test_non_select_raises(self):
        with pytest.raises(SQLGuardError, match="Only SELECT"):
            validate_and_sanitize("GRANT ALL ON employees TO hacker")


# ── Injection prevention ──────────────────────────────────────────────────────

class TestInjectionPrevention:
    def test_semicolon_stacked_query(self):
        sql = "SELECT * FROM employees; DROP TABLE employees"
        with pytest.raises(SQLGuardError, match="semicolon"):
            validate_and_sanitize(sql)

    def test_line_comment_injection(self):
        sql = "SELECT * FROM employees -- WHERE salary < 100"
        with pytest.raises(SQLGuardError, match="comment"):
            validate_and_sanitize(sql)

    def test_block_comment_injection(self):
        sql = "SELECT * FROM employees /* malicious */"
        with pytest.raises(SQLGuardError, match="comment"):
            validate_and_sanitize(sql)

    def test_hash_comment_injection(self):
        sql = "SELECT * FROM employees # bypass"
        with pytest.raises(SQLGuardError, match="comment"):
            validate_and_sanitize(sql)

    def test_empty_sql_raises(self):
        with pytest.raises(SQLGuardError, match="empty"):
            validate_and_sanitize("")

    def test_whitespace_only_raises(self):
        with pytest.raises(SQLGuardError, match="empty"):
            validate_and_sanitize("   ")
