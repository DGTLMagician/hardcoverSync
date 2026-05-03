import unittest
from unittest.mock import MagicMock, patch
from cwa_kobo_sync import CwaKoboClient, SyncManager, HardcoverClient, LocalSyncState

class TestCwaKoboClient(unittest.TestCase):
    def setUp(self):
        self.client = CwaKoboClient("http://localhost", "dummy")

    def test_parse_state_unread(self):
        library_entry = {"Title": "Test Book", "Author": "Test Author", "PageCount": "300"}
        state_entry = {"PercentRead": 0, "ReadingStatus": "Unread"}
        
        parsed = self.client.parse_state("uuid1", library_entry, state_entry)
        
        self.assertEqual(parsed["progressPercent"], 0.0)
        self.assertFalse(parsed["completed"])
        self.assertEqual(parsed["progressPages"], 0)

    def test_parse_state_reading_percentage_scale(self):
        library_entry = {"title": "Test Book 2", "PageCount": 200}
        # Kobo can send 0-100% or 0-1
        state_entry = {"percentRead": 35.5, "Status": "reading"}
        
        parsed = self.client.parse_state("uuid2", library_entry, state_entry)
        
        self.assertAlmostEqual(parsed["progressPercent"], 0.355)
        self.assertFalse(parsed["completed"])
        self.assertEqual(parsed["progressPages"], 71)

    def test_parse_state_completed(self):
        library_entry = {"title": "Test Book 3"}
        state_entry = {"ReadingStatus": "Finished"}
        
        parsed = self.client.parse_state("uuid3", library_entry, state_entry)
        self.assertTrue(parsed["completed"])

    def test_parse_state_completed_by_percentage(self):
        library_entry = {"title": "Test Book 4"}
        state_entry = {"PercentRead": 99.0}
        
        parsed = self.client.parse_state("uuid4", library_entry, state_entry)
        self.assertTrue(parsed["completed"])

class TestSyncManager(unittest.TestCase):
    def setUp(self):
        self.cwa_client = MagicMock(spec=CwaKoboClient)
        self.hc_client = MagicMock(spec=HardcoverClient)
        self.hc_client.token = "fake_token"
        self.hc_client.api_url = "http://fake_url"
        self.local_state = MagicMock(spec=LocalSyncState)
        
        self.config = {
            "DEFAULT_PRIVACY_SETTING_ID": 1,
            "STATUS_CURRENTLY_READING": 2,
            "STATUS_READ": 3,
            "STATUS_WANT_TO_READ": 1,
        }
        self.manager = SyncManager(self.cwa_client, self.hc_client, self.local_state, self.config)

    @patch("cwa_kobo_sync.search_hardcover_books")
    def test_matching_fallback(self, mock_search):
        # Scenario: book is not in local state, does not have hardcover id
        self.local_state.get_state.return_value = {}
        
        mock_search.return_value = [{"id": 999}]
        
        self.cwa_client.get_library_sync.return_value = [{"uuid": "uuid_match"}]
        self.cwa_client.get_book_state.return_value = {"PercentRead": 50}
        self.cwa_client.parse_state.return_value = {
            "sourceBookUuid": "uuid_match",
            "title": "Matchable",
            "authors": ["Author"],
            "isbn13": "1234567890123",
            "hardcoverBookId": None,
            "hardcoverEditionId": None,
            "progressPercent": 0.5,
            "progressPages": 100,
            "completed": False
        }
        
        self.hc_client.get_user_book.return_value = None
        self.hc_client.insert_user_book.return_value = {"insert_user_book": {"id": 100}}
        
        self.manager.run(dry_run=False)
        
        mock_search.assert_called_with("1234567890123", self.hc_client.token, self.hc_client.api_url)
        self.hc_client.insert_user_book.assert_called_with(999, 2, None, 1)

    @patch("cwa_kobo_sync.search_hardcover_books")
    def test_conflict_no_downgrade(self, mock_search):
        # Hardcover has it as READ (3), CWA has it as CURRENTLY READING (2)
        self.local_state.get_state.return_value = {"hardcover_book_id": 123}
        
        self.cwa_client.get_library_sync.return_value = [{"uuid": "uuid_conflict"}]
        self.cwa_client.parse_state.return_value = {
            "sourceBookUuid": "uuid_conflict",
            "title": "Conflict Book",
            "progressPercent": 0.5,
            "completed": False,
            "hardcoverBookId": 123,
            "hardcoverEditionId": None,
            "progressPages": None
        }
        
        self.hc_client.get_user_book.return_value = {
            "id": 555,
            "status_id": 3, # READ
            "user_book_reads": []
        }
        
        self.manager.run(dry_run=False)
        
        # We should NOT call update_user_book because status is READ
        self.hc_client.update_user_book.assert_not_called()
        # But we DO update the read record's progress if it existed, or insert new one.
        # Since it had no reads, we insert read record.
        self.hc_client.insert_user_book_read.assert_called_with(555, 0.5, None, False)

    def test_idempotency_hash_check(self):
        # If the hash is the same, do nothing.
        self.local_state.get_state.return_value = {
            "last_cwa_state_hash": self.manager.hash_state({
                "progressPercent": 0.5, "progressPages": 100, "completed": False
            }),
            "manual_match_required": False
        }
        
        self.cwa_client.get_library_sync.return_value = [{"uuid": "uuid_idemp"}]
        self.cwa_client.parse_state.return_value = {
            "sourceBookUuid": "uuid_idemp",
            "progressPercent": 0.5,
            "progressPages": 100,
            "completed": False
        }
        
        self.manager.run(dry_run=False)
        
        self.hc_client.get_user_book.assert_not_called()
        self.hc_client.insert_user_book.assert_not_called()
        self.hc_client.update_user_book.assert_not_called()

if __name__ == "__main__":
    unittest.main()
