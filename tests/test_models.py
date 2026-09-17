import unittest

from people_counter.models import RunResult


class RunResultTests(unittest.TestCase):
    def test_ensure_unused_marks_result_started(self):
        result = RunResult()

        result.ensure_unused()

        self.assertTrue(result.started)

    def test_ensure_unused_rejects_started_or_initialized_result(self):
        for result in (
            RunResult(started=True),
            RunResult(initialized=True),
        ):
            with self.subTest(result=result):
                with self.assertRaisesRegex(
                    RuntimeError,
                    (
                        "^RunResult already populated; "
                        "create a new config for each run$"
                    ),
                ):
                    result.ensure_unused()


if __name__ == "__main__":
    unittest.main()
