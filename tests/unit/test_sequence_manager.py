"""Unit tests for SequenceManager."""
import unittest
from unittest.mock import Mock, MagicMock, patch
from db2pgpy.sequence_manager import SequenceManager


class TestSequenceManager(unittest.TestCase):
    """Test cases for SequenceManager class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db2_connector = Mock()
        self.pg_connector = Mock()
        self.sequence_manager = SequenceManager(self.db2_connector, self.pg_connector)
    
    def test_sync_sequence_after_insert_with_data(self):
        """Test syncing sequence when table has data."""
        # Mock PostgreSQL query to return max value
        self.pg_connector.execute_query.return_value = [(5000,)]
        
        # Call sync_sequence_after_insert
        result = self.sequence_manager.sync_sequence_after_insert(
            table_name="CUSTOMERS",
            column_name="CUSTOMER_ID",
            schema="public"
        )
        
        # Assert success
        self.assertTrue(result)
        
        # Verify execute_query was called twice (MAX query and setval)
        self.assertEqual(self.pg_connector.execute_query.call_count, 2)
        
        # Verify MAX query
        first_call_args = self.pg_connector.execute_query.call_args_list[0][0][0]
        self.assertIn('MAX("CUSTOMER_ID")', first_call_args)
        self.assertIn('"CUSTOMERS"', first_call_args)
        
        # Verify setval query with correct next value (5001)
        second_call_args = self.pg_connector.execute_query.call_args_list[1][0][0]
        self.assertIn('setval', second_call_args)
        self.assertIn('customers_customer_id_seq', second_call_args)
        self.assertIn('5001', second_call_args)
    
    def test_sync_sequence_after_insert_empty_table(self):
        """Test syncing sequence when table is empty."""
        # Mock PostgreSQL query to return None (empty table)
        self.pg_connector.execute_query.return_value = [(None,)]
        
        # Call sync_sequence_after_insert
        result = self.sequence_manager.sync_sequence_after_insert(
            table_name="ORDERS",
            column_name="ORDER_ID",
            schema="public"
        )
        
        # Assert success (empty table is valid)
        self.assertTrue(result)
        
        # Verify only MAX query was called (no setval for empty table)
        self.assertEqual(self.pg_connector.execute_query.call_count, 1)
    
    def test_sync_sequence_after_insert_error_handling(self):
        """Test error handling when sync fails."""
        # Mock query to raise exception
        self.pg_connector.execute_query.side_effect = Exception("Database error")
        
        # Call sync_sequence_after_insert
        result = self.sequence_manager.sync_sequence_after_insert(
            table_name="PRODUCTS",
            column_name="PRODUCT_ID",
            schema="public"
        )
        
        # Assert failure
        self.assertFalse(result)
    
    def test_sync_sequences_for_table(self):
        """Test syncing multiple sequences for a table."""
        # Mock successful sync for each column
        self.pg_connector.execute_query.side_effect = [
            [(1000,)],  # MAX for first column
            [(1,)],     # setval result
            [(2000,)],  # MAX for second column
            [(1,)]      # setval result
        ]
        
        # Call sync_sequences_for_table with multiple PK columns
        result = self.sequence_manager.sync_sequences_for_table(
            table_name="COMPOSITE_TABLE",
            pk_columns=["ID1", "ID2"],
            schema="public"
        )
        
        # Assert 2 sequences were synced
        self.assertEqual(result, 2)
        
        # Verify execute_query was called 4 times (2 MAX + 2 setval)
        self.assertEqual(self.pg_connector.execute_query.call_count, 4)
    
    def test_sync_sequences_for_table_no_pk(self):
        """Test syncing sequences when table has no primary keys."""
        # Call sync_sequences_for_table with empty PK list
        result = self.sequence_manager.sync_sequences_for_table(
            table_name="NO_PK_TABLE",
            pk_columns=[],
            schema="public"
        )
        
        # Assert 0 sequences synced
        self.assertEqual(result, 0)
        
        # Verify no queries were executed
        self.pg_connector.execute_query.assert_not_called()
    
    def test_sync_sequences_for_table_partial_failure(self):
        """Test syncing when some sequences fail."""
        # Mock first column success, second column failure
        def query_side_effect(query):
            if 'ID1' in query:
                return [(1000,)]
            elif 'setval' in query and 'id1' in query:
                return [(1,)]
            elif 'ID2' in query:
                raise Exception("Query failed")
            return []
        
        self.pg_connector.execute_query.side_effect = query_side_effect
        
        # Call sync_sequences_for_table
        result = self.sequence_manager.sync_sequences_for_table(
            table_name="PARTIAL_FAIL_TABLE",
            pk_columns=["ID1", "ID2"],
            schema="public"
        )
        
        # Assert only 1 sequence synced (first one succeeded)
        self.assertEqual(result, 1)
    
    def test_create_sequence_for_column_return_type(self):
        """Test that create_sequence_for_column returns Optional[str]."""
        # Mock successful sequence creation
        self.pg_connector.execute_ddl = Mock()
        self.db2_connector.execute_query.return_value = [{'MAX_VAL': 100}]
        
        # Call create_sequence_for_column
        result = self.sequence_manager.create_sequence_for_column(
            table_name="TEST_TABLE",
            column_name="TEST_ID",
            start_value=1,
            schema="MAXIMO"
        )
        
        # Assert returns string (sequence name)
        self.assertIsInstance(result, str)
        self.assertEqual(result, "test_table_test_id_seq")
    
    def test_create_sequence_for_column_failure_returns_none(self):
        """Test that create_sequence_for_column returns None on failure."""
        # Mock execute_ddl to raise exception
        self.pg_connector.execute_ddl.side_effect = Exception("Cannot create sequence")
        
        # Call create_sequence_for_column
        result = self.sequence_manager.create_sequence_for_column(
            table_name="FAIL_TABLE",
            column_name="FAIL_ID",
            start_value=1,
            schema="MAXIMO"
        )
        
        # Assert returns None on failure
        self.assertIsNone(result)


    def test_get_table_sequences(self):
        """Test getting sequences for a specific table."""
        # Mock query result with 2 sequences
        self.pg_connector.execute_query.return_value = [
            ('customers_customer_id_seq', 'CUSTOMER_ID'),
            ('customers_alt_id_seq', 'ALT_ID')
        ]
        
        # Call get_table_sequences
        result = self.sequence_manager.get_table_sequences(
            table_name="CUSTOMERS",
            schema="public"
        )
        
        # Assert correct structure
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['sequence_name'], 'customers_customer_id_seq')
        self.assertEqual(result[0]['column_name'], 'CUSTOMER_ID')
        self.assertEqual(result[1]['sequence_name'], 'customers_alt_id_seq')
        self.assertEqual(result[1]['column_name'], 'ALT_ID')
    
    def test_get_table_sequences_no_sequences(self):
        """Test getting sequences when table has none."""
        # Mock empty result
        self.pg_connector.execute_query.return_value = []
        
        # Call get_table_sequences
        result = self.sequence_manager.get_table_sequences(
            table_name="NO_SEQ_TABLE",
            schema="public"
        )
        
        # Assert empty list
        self.assertEqual(result, [])
    
    def test_get_all_sequences(self):
        """Test getting all sequences in a schema."""
        # Mock query result with sequences from multiple tables
        self.pg_connector.execute_query.return_value = [
            ('CUSTOMERS', 'CUSTOMER_ID', 'customers_customer_id_seq'),
            ('ORDERS', 'ORDER_ID', 'orders_order_id_seq'),
            ('PRODUCTS', 'PRODUCT_ID', 'products_product_id_seq')
        ]
        
        # Call get_all_sequences
        result = self.sequence_manager.get_all_sequences(schema="public")
        
        # Assert correct structure
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]['table_name'], 'CUSTOMERS')
        self.assertEqual(result[0]['column_name'], 'CUSTOMER_ID')
        self.assertEqual(result[0]['sequence_name'], 'customers_customer_id_seq')
    
    def test_get_all_sequences_error_handling(self):
        """Test error handling when getting all sequences."""
        # Mock query to raise exception
        self.pg_connector.execute_query.side_effect = Exception("Database error")
        
        # Call get_all_sequences
        result = self.sequence_manager.get_all_sequences(schema="public")
        
        # Assert empty list on error
        self.assertEqual(result, [])
    
    def test_sync_all_sequences_in_schema(self):
        """Test syncing all sequences in a schema."""
        # Mock get_all_sequences to return 2 sequences
        self.pg_connector.execute_query.side_effect = [
            # get_all_sequences query
            [
                ('CUSTOMERS', 'CUSTOMER_ID', 'customers_customer_id_seq'),
                ('ORDERS', 'ORDER_ID', 'orders_order_id_seq')
            ],
            # MAX query for CUSTOMERS
            [(1000,)],
            # setval for CUSTOMERS
            [(1,)],
            # MAX query for ORDERS
            [(2000,)],
            # setval for ORDERS
            [(1,)]
        ]
        
        # Call sync_all_sequences_in_schema
        stats = self.sequence_manager.sync_all_sequences_in_schema(schema="public")
        
        # Assert correct stats
        self.assertEqual(stats['total_sequences'], 2)
        self.assertEqual(stats['synced'], 2)
        self.assertEqual(stats['failed'], 0)
    
    def test_sync_all_sequences_in_schema_filtered(self):
        """Test syncing sequences for specific tables only."""
        # Mock get_all_sequences to return 3 sequences
        self.pg_connector.execute_query.side_effect = [
            # get_all_sequences query
            [
                ('CUSTOMERS', 'CUSTOMER_ID', 'customers_customer_id_seq'),
                ('ORDERS', 'ORDER_ID', 'orders_order_id_seq'),
                ('PRODUCTS', 'PRODUCT_ID', 'products_product_id_seq')
            ],
            # MAX query for CUSTOMERS (only this one should be synced)
            [(1000,)],
            # setval for CUSTOMERS
            [(1,)]
        ]
        
        # Call sync_all_sequences_in_schema with filter
        stats = self.sequence_manager.sync_all_sequences_in_schema(
            schema="public",
            tables=["CUSTOMERS"]
        )
        
        # Assert only 1 sequence synced (filtered)
        self.assertEqual(stats['total_sequences'], 1)
        self.assertEqual(stats['synced'], 1)
        self.assertEqual(stats['failed'], 0)
    
    def test_sync_all_sequences_in_schema_no_sequences(self):
        """Test syncing when no sequences exist."""
        # Mock empty result
        self.pg_connector.execute_query.return_value = []
        
        # Call sync_all_sequences_in_schema
        stats = self.sequence_manager.sync_all_sequences_in_schema(schema="public")
        
        # Assert all zeros
        self.assertEqual(stats['total_sequences'], 0)
        self.assertEqual(stats['synced'], 0)
        self.assertEqual(stats['failed'], 0)


if __name__ == '__main__':
    unittest.main()
