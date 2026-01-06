"""Tests for schema converter module."""

import pytest
from unittest.mock import Mock
from db2pgpy.converters.schema import SchemaConverter


class TestSchemaConverter:
    """Test SchemaConverter functionality."""

    def test_generate_create_table_ddl(self):
        """Test CREATE TABLE DDL generation."""
        # Mock TypeConverter
        mock_type_converter = Mock()
        mock_type_converter.convert_type.side_effect = lambda t, l=None: {
            'INTEGER': 'INTEGER',
            'VARCHAR': f'VARCHAR({l})' if l else 'VARCHAR'
        }.get(t, t)

        converter = SchemaConverter(mock_type_converter)
        
        schema_info = {
            'columns': [
                {
                    'name': 'id',
                    'type': 'INTEGER',
                    'nullable': False,
                    'default': None,
                    'length': 4
                },
                {
                    'name': 'name',
                    'type': 'VARCHAR',
                    'nullable': True,
                    'default': None,
                    'length': 100
                }
            ]
        }

        ddl = converter.generate_create_table_ddl('users', schema_info)

        assert 'CREATE TABLE users' in ddl
        assert 'id INTEGER NOT NULL' in ddl
        assert 'name VARCHAR(100)' in ddl
        assert ddl.strip().endswith(';')

    def test_generate_create_table_with_defaults(self):
        """Test CREATE TABLE DDL with default values."""
        mock_type_converter = Mock()
        mock_type_converter.convert_type.side_effect = lambda t, l=None: t

        converter = SchemaConverter(mock_type_converter)
        
        schema_info = {
            'columns': [
                {
                    'name': 'status',
                    'type': 'VARCHAR',
                    'nullable': False,
                    'default': "'ACTIVE'",
                    'length': 20
                }
            ]
        }

        ddl = converter.generate_create_table_ddl('accounts', schema_info)

        assert "status VARCHAR NOT NULL DEFAULT 'ACTIVE'" in ddl

    def test_generate_primary_key_ddl(self):
        """Test PRIMARY KEY DDL generation."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        ddl = converter.generate_primary_key_ddl('users', ['id'])

        assert 'ALTER TABLE users' in ddl
        assert 'ADD PRIMARY KEY (id)' in ddl
        assert ddl.strip().endswith(';')

    def test_generate_composite_primary_key_ddl(self):
        """Test composite PRIMARY KEY DDL generation."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        ddl = converter.generate_primary_key_ddl('user_roles', ['user_id', 'role_id'])

        assert 'ALTER TABLE user_roles' in ddl
        assert 'ADD PRIMARY KEY (user_id, role_id)' in ddl

    def test_generate_foreign_key_ddl(self):
        """Test FOREIGN KEY DDL generation."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        fk_info = {
            'constraint_name': 'fk_user',
            'column': 'user_id',
            'referenced_table': 'users',
            'referenced_column': 'id'
        }

        ddl = converter.generate_foreign_key_ddl('orders', fk_info)

        assert 'ALTER TABLE orders' in ddl
        assert 'ADD CONSTRAINT fk_user' in ddl
        assert 'FOREIGN KEY (user_id)' in ddl
        assert 'REFERENCES users(id)' in ddl
        assert ddl.strip().endswith(';')

    def test_generate_index_ddl_unique(self):
        """Test unique INDEX DDL generation."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        index_info = {
            'name': 'idx_email',
            'table': 'users',
            'columns': ['email'],
            'unique': True
        }

        ddl = converter.generate_index_ddl(index_info)

        assert 'CREATE UNIQUE INDEX idx_email' in ddl
        assert 'ON users (email)' in ddl
        assert ddl.strip().endswith(';')

    def test_generate_index_ddl_regular(self):
        """Test regular INDEX DDL generation."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        index_info = {
            'name': 'idx_name_date',
            'table': 'users',
            'columns': ['name', 'created_date'],
            'unique': False
        }

        ddl = converter.generate_index_ddl(index_info)

        assert 'CREATE INDEX idx_name_date' in ddl
        assert 'ON users (name, created_date)' in ddl
        assert 'UNIQUE' not in ddl

    def test_schema_converter_handles_empty_columns(self):
        """Test schema converter handles empty column lists."""
        mock_type_converter = Mock()
        converter = SchemaConverter(mock_type_converter)

        schema_info = {'columns': []}
        ddl = converter.generate_create_table_ddl('empty_table', schema_info)

        assert 'CREATE TABLE empty_table' in ddl
