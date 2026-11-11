"""Options is a permissive bag: it accepts (and ignores) arbitrary engine-tuning
kwargs so existing call sites keep working."""

import unittest

import kv0


class TestOptions(unittest.TestCase):
    def test_defaults(self):
        opts = kv0.Options()
        self.assertTrue(opts.create_if_missing)

    def test_create_if_missing_flag(self):
        opts = kv0.Options(create_if_missing=False)
        self.assertFalse(opts.create_if_missing)

    def test_accepts_arbitrary_engine_kwargs(self):
        # A representative set of legacy engine-tuning kwargs a caller may pass.
        opts = kv0.Options(
            create_if_missing=True,
            paranoid_checks=False,
            keep_log_file_num=10,
            skip_stats_update_on_db_open=True,
            max_open_files=-1,
            write_buffer_size=1024 * 1024,
            db_write_buffer_size=1024 * 1024,
            max_write_buffer_number=1,
            target_file_size_base=64 * 1024 * 1024,
            max_background_compactions=4,
            num_levels=4,
        )
        self.assertEqual(opts.write_buffer_size, 1024 * 1024)
        self.assertEqual(opts.num_levels, 4)


if __name__ == "__main__":
    unittest.main()
