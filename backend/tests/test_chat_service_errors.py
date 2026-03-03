from app.services.chat_service import _build_query_error_answer


def test_invalid_object_name_returns_helpful_table_guidance():
    err = Exception("(pyodbc.ProgrammingError) Invalid object name 'Property'.")
    schema = 'Table: "dbo.Users"\n  - "Id" (int)\n\nTable: "dbo.Property_Master"\n  - "PropertyType" (nvarchar)'

    answer = _build_query_error_answer(err, schema)

    assert "Property" in answer
    assert "dbo.Users" in answer
    assert "dbo.Property_Master" in answer
    assert "schema name" in answer.lower()
